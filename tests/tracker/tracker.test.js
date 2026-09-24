// @vitest-environment jsdom
//
// Тесты static/tracker.js — раздел «Пометка трафика» корневого README.
// Сам tracker.js — старый ES5-файл без сборки, поэтому
// грузим его исходник как есть через new Function() в глобальной области
// jsdom (document/navigator/location/history — глобалы), а не через import:
// тест не должен требовать от файла модульности, которой у него нарочно нет.
//
// Пакет лежит в tests/tracker/, НЕ в static/__tests__/: Dockerfile копирует
// static/ целиком в прод-образ (COPY static/ /static/) — node_modules и прочие
// dev-зависимости туда попадать не должны.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));
const TRACKER_SRC = readFileSync(join(__dirname, '..', '..', 'static', 'tracker.js'), 'utf8');

// Все куки, которые может выставить трекер за время теста — чистим между
// тестами целиком (не только метку владельца: sid/vid тоже трекера, не гостя).
const ALL_TRACKER_COOKIES = ['_mfy_owner', '_mfy_sid', '_mfy_vid'];

function clearCookies() {
  for (const name of ALL_TRACKER_COOKIES) {
    document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/`;
  }
}

// jsdom-окно тут одно на весь файл (все it() делят один document/window), а
// tracker.js за собой регистрирует window.addEventListener('beforeunload', flush)
// и НИЧЕГО не снимает сам — так и должно быть в браузере (страница живёт одну
// загрузку). В тестах это утечка: слушатель предыдущего теста переживает его и
// остаётся висеть на window. Перехватываем addEventListener на время загрузки
// и снимаем в afterEach, чтобы каждый it() начинал с чистого window.
let capturedListeners = [];

function removeCapturedListeners() {
  for (const fn of capturedListeners) {
    window.removeEventListener('beforeunload', fn);
  }
  capturedListeners = [];
}

function loadTracker() {
  // Экземпляр предыдущего вызова (в ЭТОМ ЖЕ it(), например "загрузка страницы
  // после reload", или оставшийся от предыдущего it() — на случай если тот не
  // дошёл до сюда) снимаем ПЕРЕД регистрацией нового: иначе его 'beforeunload'
  // лишним вызовом sendBeacon аукнется в следующей проверке (см.
  // describe('изоляция экземпляров...')). afterEach ниже — финальная подстраховка.
  removeCapturedListeners();

  const realAdd = window.addEventListener.bind(window);
  const spy = vi.spyOn(window, 'addEventListener').mockImplementation((type, fn, opts) => {
    if (type === 'beforeunload') capturedListeners.push(fn);
    return realAdd(type, fn, opts);
  });

  const script = document.createElement('script');
  script.src = 'https://cdn.merfy.test/tracker.js?shop=demo-shop';
  document.body.appendChild(script);

  const sendBeacon = vi.fn(() => true);
  navigator.sendBeacon = sendBeacon;

  // eslint-disable-next-line no-new-func -- тот же файл, что уедет в браузер как есть.
  new Function(TRACKER_SRC)();

  spy.mockRestore();
  return sendBeacon;
}

// forceFlush — вместо ожидания setInterval(5с) шлём 'beforeunload', на который
// tracker.js уже подписан.
function forceFlush() {
  window.dispatchEvent(new Event('beforeunload'));
}

// sentEvents достаёт события из ПОСЛЕДНЕГО вызова sendBeacon: полезная нагрузка
// зашита в Blob, у Blob.text() jsdom/undici — асинхронный.
async function sentEvents(sendBeacon) {
  expect(sendBeacon).toHaveBeenCalled();
  const [, blob] = sendBeacon.mock.calls.at(-1);
  const text = await blob.text();
  return JSON.parse(text).events;
}

beforeEach(() => {
  vi.useFakeTimers(); // tracker.js сам заводит setInterval — реальный таймер тут не нужен
  document.body.innerHTML = '';
  clearCookies();
  localStorage.clear();
  delete navigator.webdriver;
  window.history.pushState(null, '', 'http://localhost:3000/');
});

afterEach(() => {
  removeCapturedListeners();
  vi.useRealTimers();
});

describe('навигация без сигналов', () => {
  it('обычный визит не ставит поле traffic вовсе', async () => {
    const sendBeacon = loadTracker();
    forceFlush();
    const events = await sentEvents(sendBeacon);
    for (const e of events) {
      expect(e).not.toHaveProperty('traffic');
    }
  });
});

describe('navigator.webdriver → traffic:"bot"', () => {
  it('автоматизированный браузер помечает свои события traffic:"bot"', async () => {
    Object.defineProperty(navigator, 'webdriver', { value: true, configurable: true });
    const sendBeacon = loadTracker();
    forceFlush();
    const events = await sentEvents(sendBeacon);
    expect(events.length).toBeGreaterThan(0);
    for (const e of events) {
      expect(e.traffic).toBe('bot');
    }
  });
});

describe('?mfy_owner=1 — метка владельца', () => {
  it('ставит метку, убирает параметр, сохраняет остальные параметры и #, шлёт internal', async () => {
    window.history.pushState(
      null,
      '',
      'http://localhost:3000/catalog/hats?utm_source=vk&mfy_owner=1&utm_medium=cpc#reviews',
    );

    const sendBeacon = loadTracker();

    // Параметр убран, остальные — на месте, история не перезагружает страницу.
    expect(location.search).not.toMatch(/mfy_owner/);
    expect(location.search).toContain('utm_source=vk');
    expect(location.search).toContain('utm_medium=cpc');
    expect(location.hash).toBe('#reviews');
    expect(location.pathname).toBe('/catalog/hats');

    // Метка сохранена (localStorage и/или cookie — что-то одно может быть недоступно).
    expect(localStorage.getItem('_mfy_owner')).toBe('1');

    forceFlush();
    const events = await sentEvents(sendBeacon);
    expect(events.length).toBeGreaterThan(0);
    for (const e of events) {
      expect(e.traffic).toBe('internal');
    }
  });

  it('переживает перезагрузку — метка читается из cookie/localStorage без параметра в URL', async () => {
    window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=1');
    loadTracker();

    // "Вторая загрузка той же вкладки" — параметра в адресе уже нет.
    window.history.pushState(null, '', 'http://localhost:3000/other-page');
    const sendBeacon = loadTracker();
    forceFlush();
    const events = await sentEvents(sendBeacon);
    for (const e of events) {
      expect(e.traffic).toBe('internal');
    }
  });

  // Частный случай URL-разбора: параметр — единственный в адресной строке,
  // после уборки не должно остаться висячего "?" (location.search === "").
  // Без отдельного теста регрессия наивной склейкой ("pathname + '?' + rest"
  // без проверки на пустой rest) прошла бы незамеченной.
  it('хвостовой "?" не остаётся: /?mfy_owner=1 → /', async () => {
    window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=1');
    loadTracker();
    expect(location.pathname + location.search + location.hash).toBe('/');
  });
});

describe('?mfy_owner=0 — снятие метки', () => {
  it('снимает ранее поставленную метку и убирает параметр из адреса', async () => {
    localStorage.setItem('_mfy_owner', '1');
    document.cookie = '_mfy_owner=1;path=/';

    window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=0&utm_source=vk');
    const sendBeacon = loadTracker();

    expect(location.search).not.toMatch(/mfy_owner/);
    expect(location.search).toContain('utm_source=vk');
    expect(localStorage.getItem('_mfy_owner')).not.toBe('1');

    forceFlush();
    const events = await sentEvents(sendBeacon);
    for (const e of events) {
      expect(e).not.toHaveProperty('traffic');
    }
  });
});

describe('порядок: webdriver важнее метки владельца', () => {
  it('бот с меткой владельца всё равно уходит как bot, а не internal', async () => {
    Object.defineProperty(navigator, 'webdriver', { value: true, configurable: true });
    window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=1');
    const sendBeacon = loadTracker();
    forceFlush();
    const events = await sentEvents(sendBeacon);
    for (const e of events) {
      expect(e.traffic).toBe('bot');
    }
  });
});

describe('localStorage недоступен (приватный режим)', () => {
  it('обращение к localStorage бросает исключение, но события всё равно уходят', async () => {
    const original = Object.getOwnPropertyDescriptor(window, 'localStorage');
    Object.defineProperty(window, 'localStorage', {
      configurable: true,
      get() {
        throw new Error('localStorage is blocked (private mode)');
      },
    });

    try {
      window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=1');
      const sendBeacon = loadTracker();
      forceFlush();
      const events = await sentEvents(sendBeacon);
      // Метка не смогла лечь в localStorage, но cookie — резервный канал (README
      // раздел «Пометка трафика»: "что-то одно может быть недоступно"), поэтому
      // internal всё равно есть.
      expect(events.length).toBeGreaterThan(0);
      for (const e of events) {
        expect(e.traffic).toBe('internal');
      }
    } finally {
      if (original) Object.defineProperty(window, 'localStorage', original);
    }
  });

  // И localStorage, И cookie недоступны одновременно (например, полностью
  // отключённые куки + приватный режим) — на этой САМОЙ загрузке страницы
  // события всё равно должны уйти internal: мы только что разобрали
  // ?mfy_owner=1 из адреса, и это не может зависеть от того, удалось ли
  // сохранить метку куда-либо. tracker.js держит её ещё и в переменной на
  // время загрузки страницы (README, раздел «Пометка трафика»).
  it('И localStorage, И cookie недоступны — события ЭТОЙ загрузки всё равно internal', async () => {
    const originalLS = Object.getOwnPropertyDescriptor(window, 'localStorage');
    Object.defineProperty(window, 'localStorage', {
      configurable: true,
      get() {
        throw new Error('localStorage is blocked');
      },
    });
    const originalCookieDesc = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie');
    Object.defineProperty(document, 'cookie', {
      configurable: true,
      get() {
        return '';
      },
      set() {
        /* куки отключены — запись молча не действует, как в реальном браузере */
      },
    });

    try {
      window.history.pushState(null, '', 'http://localhost:3000/?mfy_owner=1');
      const sendBeacon = loadTracker();
      forceFlush();
      const events = await sentEvents(sendBeacon);
      expect(events.length).toBeGreaterThan(0);
      for (const e of events) {
        expect(e.traffic).toBe('internal');
      }
    } finally {
      if (originalLS) Object.defineProperty(window, 'localStorage', originalLS);
      if (originalCookieDesc) Object.defineProperty(document, 'cookie', originalCookieDesc);
      else delete document.cookie;
    }
  });
});

describe('изоляция экземпляров', () => {
  // tracker.js сам регистрирует window.addEventListener('beforeunload', flush)
  // и ничего за собой не снимает — так и должно быть в браузере (страница живёт
  // одну загрузку). В тестах, где один window переживает много it(), это утечка:
  // если предыдущий экземпляр не был явно flush'нут (типичный кейс — тест
  // проверяет только URL/history.state, событий не касается), его слушатель
  // остаётся висеть и добавляет ЛИШНИЙ вызов sendBeacon в следующей проверке.
  // .at(-1) в sentEvents() эту утечку маскирует (текущий экземпляр всегда
  // регистрируется последним и потому флашится последним), поэтому здесь
  // проверяем не содержимое, а ЧИСЛО вызовов — только так утечка видна.
  it('экземпляр без flush не аукается лишним вызовом sendBeacon в следующем', () => {
    loadTracker(); // первый экземпляр — сознательно НЕ флашим

    const sendBeacon = loadTracker(); // "следующий it()"
    forceFlush();

    expect(sendBeacon.mock.calls.length).toBe(1);
  });
});

describe('history.state сохраняется при уборке ?mfy_owner', () => {
  it('не затирает history.state роутера Astro при очистке адреса', async () => {
    // Витрины на Astro держат состояние клиентского роутера в history.state;
    // history.replaceState(null, ...) стирает его и ломает переходы назад/вперёд.
    const astroRouterState = { index: 3, scrollX: 0, scrollY: 120 };
    window.history.pushState(astroRouterState, '', 'http://localhost:3000/catalog?mfy_owner=1');

    loadTracker();

    expect(location.search).not.toMatch(/mfy_owner/);
    expect(history.state).toEqual(astroRouterState);
  });

  it('то же для ?mfy_owner=0', () => {
    const astroRouterState = { index: 5 };
    window.history.pushState(astroRouterState, '', 'http://localhost:3000/cart?mfy_owner=0');

    loadTracker();

    expect(location.search).not.toMatch(/mfy_owner/);
    expect(history.state).toEqual(astroRouterState);
  });
});
