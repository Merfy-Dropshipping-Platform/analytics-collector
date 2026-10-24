// Отчёты CSP витрин новых тем (блок 13 «Секций и SDK», часть 13в; Р13б-6): report-uri политики сборки.
// Хранится счётчик по минуте: хост страницы, директива, заблокированный адрес до scheme://host, режим.
// Ни пути, ни параметров, ни IP, ни фрагмента кода покупателя.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const cspBodyMaxBytes = 16 << 10
const cspHostMaxLength = 253

// CSPReport — строка счётчика отчётов CSP.
type CSPReport struct{ Host, Directive, Blocked, Disposition string }

var cspDirectivePattern = regexp.MustCompile(`^[a-z-]{1,64}$`)
var cspSchemePattern = regexp.MustCompile(`^[a-z][a-z0-9+.-]{0,31}$`)

// Директивы и режимы — данными: выбор без цепочек if.
var cspHostSchemes = map[string]bool{"http": true, "https": true}
var cspBlockedSchemes = map[string]bool{"http": true, "https": true, "ws": true, "wss": true}
var cspDispositions = map[string]bool{"report": true, "enforce": true}

// pageHost — хост страницы из document-uri: только http и https, нижний регистр, без пути, параметров и фрагмента.
func pageHost(documentURI string) (string, error) {
	parsed, err := url.Parse(documentURI)
	if err != nil || !cspHostSchemes[strings.ToLower(parsed.Scheme)] {
		return "", fmt.Errorf("document-uri не http(s): %q", documentURI)
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" || len(host) > cspHostMaxLength {
		return "", fmt.Errorf("хост страницы пустой или длиннее %d: %q", cspHostMaxLength, host)
	}
	return host, nil
}

// reportDirective — директива нарушения: effective-directive, иначе первое слово violated-directive.
func reportDirective(fields map[string]string) (string, error) {
	// effective-directive — целиком; violated-directive (Firefox) — только первое слово.
	words := strings.Fields(fields["effective-directive"])
	if len(words) > 1 {
		return "", fmt.Errorf("директива нарушения не распознана: %q", fields["effective-directive"])
	}
	if len(words) == 0 {
		words = strings.Fields(fields["violated-directive"])
	}
	if len(words) == 0 {
		return "", fmt.Errorf("директива нарушения не указана")
	}
	directive := strings.ToLower(words[0])
	if !cspDirectivePattern.MatchString(directive) {
		return "", fmt.Errorf("директива нарушения не распознана: %q", words[0])
	}
	return directive, nil
}

// blockedOrigin — заблокированный адрес до scheme://host; неизвестный вид — схема, совсем не адрес — other.
func blockedOrigin(blockedURI string) string {
	if blockedURI == "" {
		return "unknown"
	}
	parsed, err := url.Parse(blockedURI)
	scheme := ""
	if err == nil {
		scheme = strings.ToLower(parsed.Scheme)
	}
	if cspBlockedSchemes[scheme] && parsed.Hostname() != "" {
		if len(parsed.Hostname()) > cspHostMaxLength {
			return "other"
		}
		return scheme + "://" + strings.ToLower(parsed.Hostname())
	}
	candidate := strings.ToLower(blockedURI)
	if index := strings.Index(candidate, ":"); index >= 0 {
		candidate = candidate[:index]
	}
	if cspSchemePattern.MatchString(candidate) {
		return candidate
	}
	return "other"
}

// reportDisposition — режим политики отчёта: report или enforce, иначе unknown.
func reportDisposition(fields map[string]string) string {
	disposition := strings.ToLower(fields["disposition"])
	if cspDispositions[disposition] {
		return disposition
	}
	return "unknown"
}

// ParseCSPReport: тело report-uri {"csp-report": {...}} → строка счётчика; не тот вид — ошибка.
func ParseCSPReport(body []byte) (CSPReport, error) {
	var payload struct {
		Report map[string]json.RawMessage `json:"csp-report"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Report == nil {
		return CSPReport{}, fmt.Errorf("тело не вида {\"csp-report\": {...}}")
	}
	fields := make(map[string]string, len(payload.Report))
	for name, raw := range payload.Report {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			fields[name] = value
		}
	}
	host, err := pageHost(fields["document-uri"])
	if err != nil {
		return CSPReport{}, err
	}
	directive, err := reportDirective(fields)
	if err != nil {
		return CSPReport{}, err
	}
	return CSPReport{
		Host:        host,
		Directive:   directive,
		Blocked:     blockedOrigin(fields["blocked-uri"]),
		Disposition: reportDisposition(fields),
	}, nil
}

// CSPStore записывает учтённый отчёт.
type CSPStore interface {
	Add(ctx context.Context, report CSPReport) error
}

// CSPDB — пул или транзакция pgx.
type CSPDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PoolCSPStore пишет отчёты в bronze.csp_reports.
type PoolCSPStore struct{ DB CSPDB }

// Add — вставка строки минуты; конфликт — плюс один к счётчику.
func (s PoolCSPStore) Add(ctx context.Context, report CSPReport) error {
	_, err := s.DB.Exec(ctx, `
		INSERT INTO bronze.csp_reports (minute, host, directive, blocked, disposition, reports)
		VALUES (date_trunc('minute', now()), $1, $2, $3, $4, 1)
		ON CONFLICT (minute, host, directive, blocked, disposition)
		DO UPDATE SET reports = bronze.csp_reports.reports + 1`,
		report.Host, report.Directive, report.Blocked, report.Disposition)
	return err
}

// CSPHandler — POST /csp: учёт отчёта политики витрины.
type CSPHandler struct{ Store CSPStore }

func (h CSPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, cspBodyMaxBytes)
	raw, err := io.ReadAll(body)
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	report, err := ParseCSPReport(raw)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := h.Store.Add(r.Context(), report); err != nil {
		slog.Error("csp report: store failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
