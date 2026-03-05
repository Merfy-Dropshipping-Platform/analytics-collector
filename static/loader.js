(function() {
  'use strict';

  var scripts = document.getElementsByTagName('script');
  var shopId = '';
  var baseUrl = '';
  for (var i = 0; i < scripts.length; i++) {
    var src = scripts[i].src || '';
    if (src.indexOf('loader.js') !== -1) {
      var match = src.match(/[?&]shop=([^&]+)/);
      if (match) shopId = match[1];
      var urlMatch = src.match(/^(https?:\/\/[^/]+)/);
      if (urlMatch) baseUrl = urlMatch[1];
    }
  }
  if (!shopId || !baseUrl) return;

  // Fetch active pixels
  var xhr = new XMLHttpRequest();
  xhr.open('GET', baseUrl + '/pixels?shop_id=' + shopId, true);
  xhr.onload = function() {
    if (xhr.status !== 200) return;
    try {
      var data = JSON.parse(xhr.responseText);
      if (data.pixels && data.pixels.length) {
        for (var i = 0; i < data.pixels.length; i++) {
          var pixel = data.pixels[i];
          if (pixel.inject) {
            var div = document.createElement('div');
            div.innerHTML = pixel.inject;
            var scripts = div.getElementsByTagName('script');
            for (var j = 0; j < scripts.length; j++) {
              var s = document.createElement('script');
              s.text = scripts[j].text;
              if (scripts[j].src) s.src = scripts[j].src;
              s.async = true;
              document.head.appendChild(s);
            }
          }
        }
      }
    } catch (e) {}
  };
  xhr.send();
})();
