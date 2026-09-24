(function() {
  'use strict';

  var BEACON_URL = '';
  var BATCH_INTERVAL = 5000;
  var SESSION_KEY = '_mfy_sid';
  var VISITOR_KEY = '_mfy_vid';

  // Extract shop ID from script tag
  var scripts = document.getElementsByTagName('script');
  var shopId = '';
  for (var i = 0; i < scripts.length; i++) {
    var src = scripts[i].src || '';
    if (src.indexOf('tracker.js') !== -1) {
      var match = src.match(/[?&]shop=([^&]+)/);
      if (match) shopId = match[1];
      var urlMatch = src.match(/^(https?:\/\/[^/]+)/);
      if (urlMatch) BEACON_URL = urlMatch[1] + '/collect';
    }
  }
  if (!shopId || !BEACON_URL) return;

  // Session & visitor IDs
  function getCookie(name) {
    var m = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
    return m ? decodeURIComponent(m[1]) : null;
  }
  function setCookie(name, val, days) {
    var d = new Date();
    d.setTime(d.getTime() + days * 86400000);
    document.cookie = name + '=' + encodeURIComponent(val) + ';expires=' + d.toUTCString() + ';path=/;SameSite=Lax';
  }
  function uid() {
    return 'xxxxxxxx'.replace(/x/g, function() {
      return (Math.random() * 16 | 0).toString(16);
    }) + Date.now().toString(36);
  }

  // --- Метка владельца (?mfy_owner=1 / =0) ----------------------------------
  // Свой заход магазина не должен искажать статистику. Три канала хранения:
  //   1. ownerFlagThisLoad — переменная в памяти, живёт РОВНО одну загрузку
  //      страницы (обнуляется при следующей перезагрузке скрипта). Не зависит
  //      ни от каких браузерных хранилищ — работает, даже если И localStorage,
  //      И cookie недоступны одновременно (отключены куки + приватный режим).
  //      Только что разобранный из адреса ?mfy_owner=1 обязан пометить события
  //      ЭТОЙ страницы как internal независимо от того, удалось ли его сохранить.
  //   2. localStorage — переживает переход на другую страницу и закрытие вкладки.
  //   3. cookie (365 дней) — резервный канал: что-то одно из 2 и 3 может быть
  //      недоступно (приватный режим Safari блокирует localStorage), поэтому
  //      каждое обращение — в своём try/catch, а ошибка не должна ронять
  //      остальной трекер и не должна мешать событиям уходить.
  var OWNER_KEY = '_mfy_owner';
  var ownerFlagThisLoad = false;

  function readOwnerFlag() {
    if (ownerFlagThisLoad) return true;
    try {
      if (window.localStorage && localStorage.getItem(OWNER_KEY) === '1') return true;
    } catch (e) {}
    try {
      if (getCookie(OWNER_KEY) === '1') return true;
    } catch (e) {}
    return false;
  }

  function setOwnerFlag() {
    ownerFlagThisLoad = true;
    try { if (window.localStorage) localStorage.setItem(OWNER_KEY, '1'); } catch (e) {}
    try { setCookie(OWNER_KEY, '1', 365); } catch (e) {}
  }

  function clearOwnerFlag() {
    ownerFlagThisLoad = false;
    try { if (window.localStorage) localStorage.removeItem(OWNER_KEY); } catch (e) {}
    try { setCookie(OWNER_KEY, '', -1); } catch (e) {}
  }

  // Разбираем ?mfy_owner=1|0 и сразу убираем параметр из адреса; остальные
  // параметры и # не трогаем. history.replaceState — чтобы не перезагружать
  // страницу и не плодить лишнюю запись в истории браузера.
  (function applyOwnerParam() {
    var m = location.search.match(/[?&]mfy_owner=([^&]*)/);
    if (!m) return;
    if (m[1] === '1') setOwnerFlag();
    if (m[1] === '0') clearOwnerFlag();

    var rest = location.search
      .replace(/([?&])mfy_owner=[^&]*/g, '$1')
      .replace(/^[?&]+/, '')
      .replace(/&+/g, '&')
      .replace(/&$/, '');
    var newUrl = location.pathname + (rest ? '?' + rest : '') + location.hash;
    try {
      // history.state, а не null: витрины на Astro держат состояние клиентского
      // роутера в history.state (переходы назад/вперёд); затирание в null ломает
      // навигацию по истории после самого первого захода с ?mfy_owner в адресе.
      history.replaceState(history.state, '', newUrl);
    } catch (e) {}
  })();

  // Пометка на КАЖДОЕ событие (правило 4 README, раздел «Пометка трафика»): бот важнее метки
  // владельца — если тестируем автоматизацией под своим браузером, это всё
  // равно бот. Пусто — обычный посетитель, поле traffic вовсе не ставится.
  function currentTraffic() {
    if (navigator.webdriver === true) return 'bot';
    if (readOwnerFlag()) return 'internal';
    return '';
  }

  var visitorId = getCookie(VISITOR_KEY);
  if (!visitorId) {
    visitorId = 'vis_' + uid();
    setCookie(VISITOR_KEY, visitorId, 365);
  }

  var sessionId = getCookie(SESSION_KEY);
  var isNewSession = !sessionId;
  if (!sessionId) {
    sessionId = 'sess_' + uid();
  }
  setCookie(SESSION_KEY, sessionId, 0.02); // ~30 min

  // UTM parsing
  function getParam(name) {
    var m = location.search.match(new RegExp('[?&]' + name + '=([^&]*)'));
    return m ? decodeURIComponent(m[1]) : '';
  }

  var utm = {
    utm_source: getParam('utm_source'),
    utm_medium: getParam('utm_medium'),
    utm_campaign: getParam('utm_campaign')
  };

  // Event queue
  var queue = [];

  function pushEvent(type, extra) {
    var evt = {
      type: type,
      session_id: sessionId,
      visitor_id: visitorId,
      page_url: location.pathname,
      page_title: document.title,
      referrer: document.referrer || '',
      timestamp: new Date().toISOString()
    };
    if (utm.utm_source) evt.utm_source = utm.utm_source;
    if (utm.utm_medium) evt.utm_medium = utm.utm_medium;
    if (utm.utm_campaign) evt.utm_campaign = utm.utm_campaign;
    var traffic = currentTraffic();
    if (traffic) evt.traffic = traffic;
    if (extra) {
      for (var k in extra) {
        if (extra.hasOwnProperty(k)) evt[k] = extra[k];
      }
    }
    queue.push(evt);
  }

  function flush() {
    if (queue.length === 0) return;
    var batch = queue.splice(0, 100);
    var payload = JSON.stringify({ shop_id: shopId, events: batch });

    if (navigator.sendBeacon) {
      navigator.sendBeacon(BEACON_URL, new Blob([payload], { type: 'text/plain' }));
    } else {
      var xhr = new XMLHttpRequest();
      xhr.open('POST', BEACON_URL, true);
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.send(payload);
    }
  }

  // Auto-track page views
  if (isNewSession) {
    pushEvent('session_start');
  }
  pushEvent('page_view');

  // Batch send
  setInterval(flush, BATCH_INTERVAL);
  window.addEventListener('beforeunload', flush);

  // Parse price text like "15 ₽" or "1 200 ₽" to cents integer
  function parsePriceCents(val) {
    if (typeof val === 'number') return Math.round(val * 100);
    if (typeof val !== 'string') return 0;
    var cleaned = val.replace(/[^\d.,]/g, '').replace(',', '.');
    var num = parseFloat(cleaned);
    return isNaN(num) ? 0 : Math.round(num * 100);
  }

  // Public API for e-commerce events
  window._mfy = {
    track: function(type, data) {
      pushEvent(type, data);
    },
    trackProductView: function(productId, name, price) {
      pushEvent('product_view', { product_id: productId, product_name: name, product_price: parsePriceCents(price) });
    },
    trackAddToCart: function(productId, name, price) {
      pushEvent('add_to_cart', { product_id: productId, product_name: name, product_price: parsePriceCents(price) });
    },
    trackCheckout: function() {
      pushEvent('checkout_start');
    },
    trackPurchase: function(orderId, total, products) {
      pushEvent('purchase', { order_id: orderId, order_total: total });
    }
  };
})();
