package geo

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	geoip2 "github.com/oschwald/geoip2-golang"
)

// Config carries the geo DB location and optional refresh mirrors. An empty Path disables
// geo entirely (resolver falls back to Noop). URLs are only used to (re)download the DB.
type Config struct {
	Path        string // GEO_DB_PATH — the .mmdb on a persistent volume
	URL         string // GEO_DB_URL — primary mirror (.mmdb or .mmdb.gz)
	FallbackURL string // GEO_DB_URL_FALLBACK — secondary mirror (e.g. DB-IP)
}

// LoadConfig reads the geo configuration from the environment.
func LoadConfig() Config {
	return Config{
		Path:        strings.TrimSpace(os.Getenv("GEO_DB_PATH")),
		URL:         strings.TrimSpace(os.Getenv("GEO_DB_URL")),
		FallbackURL: strings.TrimSpace(os.Getenv("GEO_DB_URL_FALLBACK")),
	}
}

// Setup builds a Resolver and NEVER returns an error: no path, a missing file, a failed
// download, or a corrupt DB all degrade to the Noop provider so /collect keeps working with
// geo = null. It runs once at startup and may block on a download — never in the hot path.
func Setup(ctx context.Context, cfg Config) *Resolver {
	p, err := buildProvider(ctx, cfg)
	if err != nil {
		slog.Warn("geo disabled (resolver falls back to noop)", "error", err)
		return NewResolver(NoopProvider{})
	}
	return NewResolver(p)
}

func buildProvider(ctx context.Context, cfg Config) (Provider, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("GEO_DB_PATH not set")
	}
	if _, err := os.Stat(cfg.Path); err != nil {
		if derr := ensureDownloaded(ctx, cfg); derr != nil {
			return nil, fmt.Errorf("geo db missing at %q and download failed: %w", cfg.Path, derr)
		}
	}
	return OpenMMDB(cfg.Path)
}

// ensureDownloaded fetches the mmdb to cfg.Path, trying the primary mirror then the fallback.
func ensureDownloaded(ctx context.Context, cfg Config) error {
	var urls []string
	if cfg.URL != "" {
		urls = append(urls, cfg.URL)
	}
	if cfg.FallbackURL != "" {
		urls = append(urls, cfg.FallbackURL)
	}
	if len(urls) == 0 {
		return fmt.Errorf("no GEO_DB_URL / GEO_DB_URL_FALLBACK configured")
	}
	var lastErr error
	for _, u := range urls {
		if err := downloadMMDB(ctx, u, cfg.Path); err != nil {
			slog.Warn("geo db download failed", "url", u, "error", err)
			lastErr = err
			continue
		}
		slog.Info("geo db downloaded", "url", u, "path", cfg.Path)
		return nil
	}
	return lastErr
}

// downloadMMDB GETs url (5 min timeout), gunzips when the payload is gzip (by magic 0x1f8b
// or a .gz URL suffix), VALIDATES it opens as a real mmdb, then atomically renames it into
// place. It never leaves a partial or corrupt file at dest. tar.gz is not supported.
func downloadMMDB(ctx context.Context, url, dest string) error {
	dctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(dctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %q: status %d", url, resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	// Temp file in the SAME directory (same filesystem) so the final rename is atomic.
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".geo-*.mmdb.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename has consumed it

	br := bufio.NewReader(resp.Body)
	var src io.Reader = br
	if magic, _ := br.Peek(2); (len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b) || strings.HasSuffix(url, ".gz") {
		gz, gerr := gzip.NewReader(br)
		if gerr != nil {
			tmp.Close()
			return fmt.Errorf("gzip open: %w", gerr)
		}
		defer gz.Close()
		src = gz
	}

	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Validate BEFORE swapping the live file — a garbage download must not replace a good DB.
	probe, err := geoip2.Open(tmpName)
	if err != nil {
		return fmt.Errorf("downloaded file is not a valid mmdb: %w", err)
	}
	probe.Close()

	return os.Rename(tmpName, dest)
}

// Bootstrap loads the geo DB (opening an existing file or downloading it) and hot-swaps it into
// the resolver. Meant to run in a background goroutine AFTER the HTTP server is up, so a
// first-boot download (up to 5 min) never blocks /health or /collect readiness — the resolver
// serves geo=null (noop) until this completes. On any error the resolver stays as-is (noop).
func (r *Resolver) Bootstrap(ctx context.Context, cfg Config) error {
	p, err := buildProvider(ctx, cfg)
	if err != nil {
		return err
	}
	r.SwapProvider(p)
	return nil
}

// Refresh re-downloads the mmdb (primary→fallback), opens it, and hot-swaps the provider.
// On ANY error the current provider stays live — a refresh never degrades a working resolver.
func (r *Resolver) Refresh(ctx context.Context, cfg Config) error {
	if err := ensureDownloaded(ctx, cfg); err != nil {
		return err
	}
	p, err := OpenMMDB(cfg.Path)
	if err != nil {
		return err
	}
	r.SwapProvider(p)
	slog.Info("geo db refreshed", "provider", r.ProviderName())
	return nil
}

// StartRefreshLoop periodically refreshes the geo DB from the mirrors. It is a no-op when no
// download URL is configured (a baked-in / volume-only DB), so it never fights a static file.
func StartRefreshLoop(ctx context.Context, r *Resolver, cfg Config, interval time.Duration) {
	if cfg.URL == "" && cfg.FallbackURL == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Refresh(ctx, cfg); err != nil {
					slog.Warn("geo refresh failed (keeping current db)", "error", err)
				}
			}
		}
	}()
}
