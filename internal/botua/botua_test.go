package botua

import "testing"

// TestIsBot_KnownBots — реальные подписи браузера роботов: поисковики, соцсети
// и мессенджеры с превью ссылок, проверки скорости и мониторинг, HTTP-клиенты
// и библиотеки, ИИ-агенты, общие слова. Каждая подпись — реальный формат этих
// ботов (не выдумана), чтобы тест доказывал распознавание настоящих UA, а не
// своих же токенов. Headless-автоматика (Selenium/Puppeteer/Playwright) — в
// TestIsBot_HeadlessAutomationRealSignatures и TestIsBot_AutomationInsuranceMarkers
// отдельно, см. их комментарии.
func TestIsBot_KnownBots(t *testing.T) {
	cases := []string{
		// поисковики
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
		"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
		"Mozilla/5.0 (compatible; YandexMetrika/3.0; +http://yandex.com/bots)",
		"Mozilla/5.0 (compatible; YandexAccessibilityBot/3.0; +http://yandex.com/bots)",
		"Mozilla/5.0 (compatible; YandexRenderResourcesBot/1.0; +http://yandex.com/bots)",
		"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
		"Mozilla/5.0 (compatible; Mail.RU_Bot/2.0; +http://go.mail.ru/help/robots)",
		"DuckDuckBot/1.1; (+http://duckduckgo.com/duckduckbot.html)",
		"Mozilla/5.0 (Applebot/0.1; +http://www.apple.com/go/applebot)",
		"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
		// доп. краулеры Google — у каждого своя задача, GoogleBot их не покрывает
		"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.123 Mobile Safari/537.36 (compatible; Google-InspectionTool/1.0)",
		"Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GoogleOther)",
		"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.123 Mobile Safari/537.36 (compatible; Google-Read-Aloud; +https://support.google.com/webmasters/answer/1061943)",
		"Mediapartners-Google",
		// Naver (Yeti) и другие зарубежные поисковики
		"Yeti/1.1 (+http://naver.me/bot)",
		// соцсети и мессенджеры (превью ссылок)
		"facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
		"TelegramBot (like TwitterBot)",
		"WhatsApp/2.19.81 A",
		"Mozilla/5.0 (compatible; vkShare; +http://vk.com/dev/Share)",
		"Twitterbot/1.0",
		"Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
		"iframely/1.3.0",
		"Mozilla/5.0 (compatible; SkypeUriPreview Preview/0.5)",
		// проверки скорости, мониторинг, пререндер
		"Mozilla/5.0 (Linux; Android 7.0; Moto G (4)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.107 Mobile Safari/537.36 Chrome-Lighthouse",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) PageSpeed/1.9 Safari/537.36",
		"Mozilla/5.0 (compatible; GTmetrix)",
		"Mozilla/5.0 (compatible; PingdomBot/1.4; +http://www.pingdom.com/)",
		"Mozilla/5.0+(compatible; UptimeRobot/2.0; http://www.uptimerobot.com/)",
		"DatadogSynthetics",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Prerender (+https://github.com/prerender/prerender)",
		// ИИ-агенты, которые ходят по ссылкам от лица пользователя
		"Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; Perplexity-User/1.0; +https://docs.perplexity.ai/docs/perplexity-crawlers",
		// HTTP-клиенты и библиотеки (никогда не настоящий браузер)
		"curl/7.79.1",
		"Wget/1.21.1",
		"python-requests/2.28.1",
		"Python-urllib/3.9",
		"python-httpx/0.25.0",
		"Python/3.11 aiohttp/3.8.5",
		"Scrapy/2.11.0 (+https://scrapy.org)",
		"Go-http-client/1.1",
		"okhttp/4.9.0",
		"axios/1.4.0",
		"node-fetch",
		"undici",
		"Java/1.8.0_281",
		"libwww-perl/6.15",
		"PostmanRuntime/7.32.3",
		"HTTPie/3.2.2",
		"Deno/1.37.0",
		"Dart/3.1 (dart:io)",
		// общие слова (не попали в явные списки выше)
		"Mozilla/5.0 (compatible; SomeUnknownCrawler/1.0)",
		"Mozilla/5.0 (compatible; SomeSpider/1.0)",
		"Yahoo! Slurp",
	}
	for _, ua := range cases {
		if !IsBot(ua) {
			t.Errorf("IsBot(%q) = false; want true (known bot signature)", ua)
		}
	}
}

// TestIsBot_HeadlessAutomationRealSignatures — реальные подписи headless-браузеров.
// HeadlessChrome и PhantomJS действительно самоподписываются этими токенами.
func TestIsBot_HeadlessAutomationRealSignatures(t *testing.T) {
	cases := []string{
		"Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/119.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/119.0.6045.105 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/120.0.6099.71 Safari/537.36",
		"Mozilla/5.0 (Unknown; Linux x86_64) AppleWebKit/538.1 (KHTML, like Gecko) PhantomJS/2.1.1 Safari/538.1",
	}
	for _, ua := range cases {
		if !IsBot(ua) {
			t.Errorf("IsBot(%q) = false; want true (реальная подпись headless-браузера)", ua)
		}
	}
}

// TestIsBot_AutomationInsuranceMarkers — Selenium/Puppeteer/Playwright ПО
// УМОЛЧАНИЮ своё имя в User-Agent не пишут: Selenium управляет обычным
// Chrome/ChromeDriver с его родным UA, Puppeteer/Playwright по умолчанию дают
// HeadlessChrome (см. TestIsBot_HeadlessAutomationRealSignatures) или полноценный
// Chrome в headless=new режиме. Строки ниже — СИНТЕТИЧЕСКИЕ (не captured-трафик):
// они проверяют лишь то, что если кто-то явно передаст такой UA (кастомная
// настройка, самодельный скрипт), маркер сработает как страховка. Основной
// сигнал для автоматики — клиентский navigator.webdriver (tracker.js ставит
// traffic:"bot", см. README).
func TestIsBot_AutomationInsuranceMarkers(t *testing.T) {
	cases := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Selenium/4.15.0",
		"Mozilla/5.0 (X11; Linux x86_64) Puppeteer/21.0.0",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Playwright/1.40.0",
	}
	for _, ua := range cases {
		if !IsBot(ua) {
			t.Errorf("IsBot(%q) = false; want true (маркер-страховка)", ua)
		}
	}
}

// TestIsBot_EmptyIsBot — пустая подпись браузера считается ботом (настоящий
// браузер её всегда посылает; см. README, раздел «Пометка трафика», правило 4.1).
func TestIsBot_EmptyIsBot(t *testing.T) {
	if !IsBot("") {
		t.Error("IsBot(\"\") = false; want true (empty UA is a bot)")
	}
}

// TestIsBot_RealBrowsers — настоящие браузеры НЕ должны попадать в список ботов.
func TestIsBot_RealBrowsers(t *testing.T) {
	cases := []string{
		// Chrome (Windows)
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
		// Safari iOS
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
		// Яндекс Браузер (использует YaBrowser/Yowser, а не Yandex)
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 YaBrowser/23.9.1.1210 Yowser/2.5 Safari/537.36",
		// Firefox
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
		// Samsung Internet
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/115.0.0.0 Mobile Safari/537.36",
		// Chrome Android
		"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36",
		// Safari macOS
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
		// Edge (Chromium)
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46",
	}
	for _, ua := range cases {
		if IsBot(ua) {
			t.Errorf("IsBot(%q) = true; want false (real browser)", ua)
		}
	}
}

// TestIsBot_YandexRealClientsNotBots — маркер Яндекса анкерован префиксом
// "compatible; " (см. botMarkers): живые Яндекс-клиенты — YaApp_Android/iOS
// шлют внутри себя токен "YandexSearch", в котором тоже есть "Yandex", но без
// "compatible; " перед ним. Настоящие роботы Яндекса подписываются строго
// форматом "Mozilla/5.0 (compatible; YandexXxx/…)" — этим и отличаются от
// собственных клиентов Яндекса.
func TestIsBot_YandexRealClientsNotBots(t *testing.T) {
	cases := []string{
		// Яндекс Браузер Android
		"Mozilla/5.0 (Linux; Android 13; SM-G991B Build/TP1A.220624.014) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.163 YaBrowser/23.9.5.176.00 SA/3 Mobile Safari/537.36",
		// Яндекс Браузер iOS
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 YaBrowser/23.10.0.0 Mobile/15E148 Safari/604.1",
		// Приложение Яндекса Android (YaApp_Android + YaSearchBrowser + внутренний YandexSearch)
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 YaApp_Android/23.112 YaSearchBrowser/23.112 YandexSearch/23.112 Mobile Safari/537.36",
		// Приложение Яндекса iOS
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 YandexSearch/900.11.1",
	}
	for _, ua := range cases {
		if IsBot(ua) {
			t.Errorf("IsBot(%q) = true; want false (настоящий клиент Яндекса, не робот)", ua)
		}
	}
}

// TestIsBot_ModelNamesAndInAppBrowsersNotBots — общее слово "bot" случайно
// совпадает с моделью телефона CUBOT ("CUBOT X30" содержит "bot"); маркер
// вырезается из подписи перед проверкой (falsePositiveMarkers). Остальные
// случаи — встроенные браузеры соцсетей/мессенджеров и других моделей
// телефонов — регрессия: они не должны ломаться ни сейчас, ни в будущем.
func TestIsBot_ModelNamesAndInAppBrowsersNotBots(t *testing.T) {
	cases := []string{
		// CUBOT — модель телефона, "bot" — часть названия, не робот
		"Mozilla/5.0 (Linux; Android 10; CUBOT X30) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36",
		// Встроенный браузер Telegram Android
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/116.0.0.0 Mobile Safari/537.36 Telegram-Android/10.2.0 (SM-G991B; Android 13; SDK 33; AVERAGE)",
		// Встроенный браузер VK Android
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/116.0.0.0 Mobile Safari/537.36 VKAndroidApp/8.9-13530 (Samsung, SM-G991B; Android 13; Scale 3.0; 1080x2265)",
		// Встроенный браузер Instagram
		"Mozilla/5.0 (Linux; Android 13; SM-G991B Build/TP1A.220624.014; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/116.0.0.0 Mobile Safari/537.36 Instagram 302.0.0.23.114 Android (33/13; 420dpi; 1080x2265; samsung; SM-G991B; o1s; exynos2100; en_US; 508284165)",
		// Встроенный браузер Facebook (FBAN/FBAV)
		"Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/116.0.0.0 Mobile Safari/537.36 [FBAN/FB4A;FBAV/440.0.0.32.115;FBBV/490371489;]",
		// MIUI Browser
		"Mozilla/5.0 (Linux; Android 12; M2101K6G) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/107.0.5304.141 Mobile Safari/537.36 XiaoMi/MiuiBrowser/17.5.310123",
	}
	for _, ua := range cases {
		if IsBot(ua) {
			t.Errorf("IsBot(%q) = true; want false (модель телефона/встроенный браузер, не робот)", ua)
		}
	}
}

// TestIsBot_ServerUAIsNotBot — заказы шлёт сервер, не браузер (README, раздел
// «Пометка трафика»): подпись Node.js "node" сама по себе НЕ входит в список
// ботов (иначе граница между «сервер прислал заказ» и «робот пришёл в браузер»
// стёрлась бы). Обход правила 4.1 для purchase/order_cancel живёт в
// internal/handler.classifyTraffic — этот тест только про сам IsBot.
func TestIsBot_ServerUAIsNotBot(t *testing.T) {
	if IsBot("node") {
		t.Error(`IsBot("node") = true; want false (подпись сервера заказов — не бот сама по себе)`)
	}
}

// BenchmarkIsBot — узнаём стоимость классификации на запрос. Регэксп с ~50
// альтернативами на подпись до ~250 байт был ощутимо (~на порядок) медленнее
// прямого strings.Contains по строчным маркерам; README и отчёт куска
// фиксируют конкретные цифры на этой машине.
func BenchmarkIsBot(b *testing.B) {
	uas := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsBot(uas[i%len(uas)])
	}
}
