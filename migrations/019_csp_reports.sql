-- Migration 019: отчёты CSP витрин новых тем (блок 13 «Секций и SDK», часть 13в), POST /csp: счётчик по минуте.
-- Хост страницы, директива, заблокированный адрес до scheme://host, режим; ни пути, ни IP, ни фрагмента кода.
-- Хранится 30 дней (internal/db/maintenance.go, PruneCSPReports).
CREATE TABLE IF NOT EXISTS bronze.csp_reports (
    minute      TIMESTAMPTZ NOT NULL,
    host        TEXT NOT NULL,
    directive   TEXT NOT NULL,
    blocked     TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('report', 'enforce', 'unknown')),
    reports     INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (minute, host, directive, blocked, disposition)
);
CREATE INDEX IF NOT EXISTS idx_csp_reports_host_minute ON bronze.csp_reports (host, minute);
