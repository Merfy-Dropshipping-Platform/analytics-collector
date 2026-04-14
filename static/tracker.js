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

  // Public API for e-commerce events
  window._mfy = {
    track: function(type, data) {
      pushEvent(type, data);
    },
    trackProductView: function(productId, name, price) {
      pushEvent('product_view', { product_id: productId, product_name: name, product_price: price });
    },
    trackAddToCart: function(productId, name, price) {
      pushEvent('add_to_cart', { product_id: productId, product_name: name, product_price: price });
    },
    trackCheckout: function() {
      pushEvent('checkout_start');
    },
    trackPurchase: function(orderId, total, products) {
      pushEvent('purchase', { order_id: orderId, order_total: total });
    }
  };
})();
