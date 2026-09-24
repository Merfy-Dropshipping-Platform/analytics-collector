// Package botua распознаёт роботов по подписи браузера (User-Agent) запроса
// /collect. Список — данные (срез строк), не цепочка if.
//
// Реализация — strings.Contains по строчным маркерам, а не регэксп: с полусотней
// альтернатив (?i)(a|b|c|...) на подпись длиной до ~250 байт регэксп заметно —
// на порядок — медленнее прямого сравнения строк (см. BenchmarkIsBot). Подпись
// приводится к нижнему регистру ОДИН раз на вызов, маркеры уже хранятся
// строчными — сравнение без учёта регистра получается бесплатно, без
// regexp.MustCompile(`(?i)...`).
package botua

import "strings"

// botMarkers — куски подписи (строчными!), по которым запрос считается роботом.
// Группы:
//   - поисковики (включая узкоспециальные краулеры Google: InspectionTool —
//     проверка индексации, GoogleOther — служебный краулер вне основного
//     индекса, Google-Read-Aloud — озвучка, Mediapartners-Google — AdSense);
//   - соцсети, мессенджеры и сервисы превью ссылок;
//   - проверки скорости, мониторинг, пререндер;
//   - ИИ-агенты, которые ходят по ссылкам от лица пользователя;
//   - headless-автоматика: HeadlessChrome/PhantomJS — реальные самоподписи;
//     Selenium/Puppeteer/Playwright эти инструменты по умолчанию в UA НЕ
//     пишут (Selenium — обычный Chrome/ChromeDriver, Puppeteer/Playwright —
//     HeadlessChrome или обычный Chrome в headless=new) — токены оставлены
//     как страховка на случай кастомной настройки, а не основной сигнал;
//     основной сигнал для автоматики — клиентский navigator.webdriver
//     (tracker.js ставит traffic:"bot", см. README);
//   - HTTP-клиенты и библиотеки (никогда не настоящий браузер). Голого "node"
//     здесь нарочно нет: сервер заказов шлёт purchase/order_cancel именно с
//     такой подписью, и это не должно классифицировать оплату как робота —
//     обход для этих двух типов событий сделан в internal/handler.classifyTraffic;
//   - общие слова, которые не попали в явные списки выше.
//
// "compatible; yandex" анкерован префиксом: все роботы Яндекса подписываются
// строго форматом "Mozilla/5.0 (compatible; YandexXxx/…)", а живые Яндекс-клиенты
// (YaBrowser, приложение Яндекса) — нет, даже когда сами несут подстроку "yandex"
// (см. TestIsBot_YandexRealClientsNotBots).
var botMarkers = []string{
	// поисковики
	"googlebot", "compatible; yandex", "bingbot", "mail.ru_bot", "duckduckbot", "applebot", "baiduspider",
	"google-inspectiontool", "googleother", "google-read-aloud", "mediapartners-google", "yeti/",
	// соцсети, мессенджеры, превью ссылок
	"facebookexternalhit", "telegrambot", "whatsapp", "vkshare", "twitterbot", "slackbot", "iframely", "skypeuripreview",
	// проверки скорости, мониторинг, пререндер
	"lighthouse", "pagespeed", "gtmetrix", "pingdom", "uptimerobot", "datadogsynthetics", "prerender",
	// ИИ-агенты
	"perplexity-user",
	// headless-автоматика
	"headlesschrome", "phantomjs", "selenium", "puppeteer", "playwright",
	// HTTP-клиенты и библиотеки
	"curl", "wget", "python-requests", "python-urllib", "python-httpx", "aiohttp", "scrapy",
	"go-http-client", "okhttp", "axios", "node-fetch", "undici", "java/", "libwww",
	"postmanruntime", "httpie", "deno/", "dart/",
	// общие слова — ловят роботов, не попавших в явные списки выше
	"bot", "crawler", "spider", "crawl", "slurp",
}

// falsePositiveMarkers — куски НАСТОЯЩИХ подписей браузеров, которые случайно
// содержат общие слова из botMarkers, но роботами не являются (например,
// телефоны CUBOT — "bot" внутри названия модели). Вырезаем их из подписи ПЕРЕД
// проверкой на список ботов. Данные, а не отдельная ветка if на каждый случай:
// список пополняется по мере находок.
var falsePositiveMarkers = []string{
	"cubot", // телефоны CUBOT — "bot" в модели, не робот
}

// IsBot сообщает, была ли подпись браузера ботом. Пустая подпись — тоже бот:
// настоящий браузер её никогда не присылает.
func IsBot(userAgent string) bool {
	if userAgent == "" {
		return true
	}
	lower := strings.ToLower(userAgent)
	for _, fp := range falsePositiveMarkers {
		lower = strings.ReplaceAll(lower, fp, "")
	}
	for _, m := range botMarkers {
		if strings.Contains(lower, m) {
			return true
		}
	}
	return false
}
