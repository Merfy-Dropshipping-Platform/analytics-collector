package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const cspPage = `"document-uri":"https://shop-1.dev.merfy.ru/"`

func TestParseCSPReport(t *testing.T) {
	cases := []struct {
		name, body string
		want       CSPReport
	}{
		{
			"Chromium: хост страницы без пути, параметров и фрагмента; адрес — до хоста",
			`{"csp-report":{"document-uri":"https://Shop-1.dev.merfy.ru/products/scarf/?utm_source=vk#top","effective-directive":"script-src-elem","violated-directive":"script-src-elem","blocked-uri":"https://evil.example/x.js?id=1","disposition":"report"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "script-src-elem", "https://evil.example", "report"},
		},
		{
			"Firefox: директива — первое слово violated-directive, режима нет",
			`{"csp-report":{` + cspPage + `,"violated-directive":"script-src 'self'","blocked-uri":"inline"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "script-src", "inline", "unknown"},
		},
		{
			"data: и обязательный заголовок",
			`{"csp-report":{` + cspPage + `,"effective-directive":"img-src","blocked-uri":"data","disposition":"enforce"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "img-src", "data", "enforce"},
		},
		{
			"расширение браузера — только схема",
			`{"csp-report":{` + cspPage + `,"effective-directive":"script-src-elem","blocked-uri":"chrome-extension://abcdef/content.js","disposition":"report"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "script-src-elem", "chrome-extension", "report"},
		},
		{
			"пустой адрес — unknown",
			`{"csp-report":{` + cspPage + `,"effective-directive":"script-src-elem","blocked-uri":"","disposition":"report"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "script-src-elem", "unknown", "report"},
		},
		{
			"адрес не адрес — other",
			`{"csp-report":{` + cspPage + `,"effective-directive":"script-src-elem","blocked-uri":"<<<","disposition":"report"}}`,
			CSPReport{"shop-1.dev.merfy.ru", "script-src-elem", "other", "report"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseCSPReport([]byte(c.body))
			if err != nil || got != c.want {
				t.Fatalf("ParseCSPReport = %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}

func TestParseCSPReport_Rejects(t *testing.T) {
	long := strings.Repeat("a", cspHostMaxLength+1)
	for name, body := range map[string]string{
		"не JSON":              `csp`,
		"нет csp-report":       `{"report":{}}`,
		"страница не http(s)":  `{"csp-report":{"document-uri":"about:blank","effective-directive":"script-src-elem"}}`,
		"нет директивы":        `{"csp-report":{` + cspPage + `}}`,
		"директива с пробелом": `{"csp-report":{` + cspPage + `,"effective-directive":"script src"}}`,
		"хост длиннее 253":     `{"csp-report":{"document-uri":"https://` + long + `/","effective-directive":"script-src-elem"}}`,
	} {
		if _, err := ParseCSPReport([]byte(body)); err == nil {
			t.Errorf("%s: отчёт принят", name)
		}
	}
}

type fakeCSPStore struct {
	reports []CSPReport
	err     error
}

func (s *fakeCSPStore) Add(_ context.Context, report CSPReport) error {
	if s.err != nil {
		return s.err
	}
	s.reports = append(s.reports, report)
	return nil
}

func postCSP(store CSPStore, body string) int {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/csp", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/csp-report")
	CSPHandler{Store: store}.ServeHTTP(recorder, request)
	return recorder.Code
}

func TestCSPHandler(t *testing.T) {
	sample := `{"csp-report":{` + cspPage + `,"effective-directive":"script-src-elem","blocked-uri":"inline","disposition":"report"}}`
	store := &fakeCSPStore{}
	if code := postCSP(store, sample); code != http.StatusNoContent || len(store.reports) != 1 {
		t.Fatalf("отчёт: код %d, строк %d; want 204 и 1", code, len(store.reports))
	}
	if code := postCSP(store, `{}`); code != http.StatusBadRequest {
		t.Errorf("не отчёт: код %d; want 400", code)
	}
	big := `{"csp-report":{"script-sample":"` + strings.Repeat("x", cspBodyMaxBytes) + `"}}`
	if code := postCSP(store, big); code != http.StatusRequestEntityTooLarge {
		t.Errorf("тело больше предела: код %d; want 413", code)
	}
	if code := postCSP(&fakeCSPStore{err: errors.New("db down")}, sample); code != http.StatusServiceUnavailable {
		t.Errorf("база не ответила: код %d; want 503", code)
	}
}
