# Тесты static/tracker.js

Кусок 114-P1 «Пометка трафика» (см. корневой [README](../../README.md), раздел
«Пометка трафика»).

## Почему не в static/__tests__

`Dockerfile` копирует `static/` целиком в прод-образ (`COPY static/ /static/`,
см. `Dockerfile:17`). Тестовый пакет с `node_modules` внутри `static/` уехал бы
в образ вместе с трекером — поэтому тесты лежат здесь, в `tests/tracker/`, а
`static/` содержит только сам `tracker.js` и `loader.js`.

## Как запустить

```bash
cd tests/tracker
pnpm install
pnpm test
```

Зависимости — `vitest` + `jsdom`, версии зафиксированы в `pnpm-lock.yaml`.

## Как это устроено

`tracker.js` — старый файл в ES5 без сборки (специально: витрины подключают
его напрямую тегом `<script>`, никакого бандлера в рантайме нет). Тесты грузят
его исходник как текст и выполняют через `new Function(source)()` внутри
jsdom-окружения — `document`/`navigator`/`location`/`history` там уже глобалы,
ровно как в браузере. `navigator.sendBeacon` подменяется на мок, полезная
нагрузка — зашитый в `Blob` JSON.

jsdom-окно одно на весь файл (все `it()` в нём делят один `document`/`window`),
поэтому `loadTracker()` в каждом тесте сам снимает `beforeunload`-слушателя
предыдущего вызова перед регистрацией нового (см. `removeCapturedListeners` —
иначе он остаётся висеть и аукается лишним вызовом `sendBeacon` в следующей
проверке), а `afterEach` подчищает всё, что осталось после последнего вызова
в тесте.
