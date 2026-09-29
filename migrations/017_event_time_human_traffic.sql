-- Migration 017: качество данных аналитики (spec 114, кусок P2).
--
-- Формулы, границы суток и периодов — прежние. Меняются только данные, из которых они считаются:
--   1. Трафик — только люди. Посетители, визиты, просмотры, воронка, каналы, гео считаются по
--      событиям с traffic_type = 'human' (колонка из 016): роботы и свои не входят.
--      Деньги — покупки и отмены (purchase, order_cancel) — все, без фильтра: это деньги.
--      В представлениях, где рядом трафик и деньги, фильтр один:
--        traffic_type = 'human' OR event_type IN ('purchase', 'order_cancel').
--   2. День — по времени события (event_timestamp), а не записи в базу (created_at): когда запись
--      стояла (13–15.09, ~35 часов), визиты 14.09 уезжали на 15.09. Сутки режутся тем же
--      date_trunc('day', …), что и раньше, — по поясу сессии базы.
--   3. Окна — тоже по времени события: interval '13 months' — столько же, сколько теперь хранятся
--      сырые события (db.RetentionMonths; тест сверяет); у «Топа товаров» — прежние 30 дней.
--   4. Защита от «сиротских» отмен (014) сохранена: отмена вычитается, только если у заказа была
--      покупка.
--   5. Уникальные индексы — именованные, IF NOT EXISTS: нужны для REFRESH … CONCURRENTLY и не
--      копятся от старта к старту (013).
--
-- Идемпотентно и последней по номеру: entrypoint.sh на каждом старте перезапускает все миграции,
-- 009/010/012/014 при этом пересоздают старые определения — эта миграция идёт после них и каждый
-- раз ставит итоговые. Всё в одной транзакции: если что-то упадёт, останутся прежние
-- представления, а не половина новых.

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS gold.dashboard_kpi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.top_products CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_traffic CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_orders CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_funnel CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_channel_attribution CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_geo CASCADE;


-- Трафик за день: просмотры, визиты, посетители, новые сессии — только люди.
-- Формулы — из 012. Посетители — по тем же событиям визита, что и визиты (28.09): покупка от
-- сервиса заказов без визита браузера (visitor_id "server-…") посетителем не считается.
CREATE MATERIALIZED VIEW silver.daily_traffic AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', event_timestamp)::date AS day,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view', 'session_start')) AS unique_sessions,
    COUNT(DISTINCT visitor_id) FILTER (WHERE event_type IN ('page_view', 'session_start')) AS unique_visitors,
    COUNT(*) FILTER (WHERE event_type = 'session_start') AS new_sessions
FROM bronze.events
WHERE traffic_type = 'human'
  AND event_timestamp >= now() - interval '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', event_timestamp)::date;

CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_traffic ON silver.daily_traffic (shop_id, day);


-- Деньги за день: чистые заказы и выручка (покупки минус отмены), средний чек — все события,
-- без фильтра по пометке трафика. Дубли событий заказа не удваивают сумму (010), отмена без
-- покупки не вычитается (014). valid_orders — без окна: покупка чуть старше 13 месяцев ещё может
-- лежать в неудалённой партиции, и её отмена внутри окна вычтется — это законный возврат (014).
CREATE MATERIALIZED VIEW silver.daily_orders AS
WITH valid_orders AS (
  SELECT DISTINCT shop_id, order_id
  FROM bronze.events
  WHERE event_type = 'purchase' AND order_id IS NOT NULL
),
deduped AS (
  SELECT
    e.shop_id,
    e.tenant_id,
    date_trunc('day', e.event_timestamp)::date AS day,
    e.order_id,
    e.event_type,
    MAX(e.order_total_cents) AS order_total_cents,
    MAX(e.session_id) AS session_id
  FROM bronze.events e
  WHERE e.event_type IN ('purchase', 'order_cancel')
    AND e.order_id IS NOT NULL
    AND e.event_timestamp >= now() - interval '13 months'
    AND (
      e.event_type = 'purchase'
      OR EXISTS (
        SELECT 1 FROM valid_orders v
        WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
      )
    )
  GROUP BY e.shop_id, e.tenant_id, date_trunc('day', e.event_timestamp)::date, e.order_id, e.event_type
)
SELECT
  shop_id,
  tenant_id,
  day,
  COUNT(*) FILTER (WHERE event_type = 'purchase')
    - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    AS order_count,
  COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
    - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    AS total_revenue_cents,
  CASE
    WHEN (COUNT(*) FILTER (WHERE event_type = 'purchase')
          - COUNT(*) FILTER (WHERE event_type = 'order_cancel')) > 0
    THEN (
      COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
      - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    ) / (
      COUNT(*) FILTER (WHERE event_type = 'purchase')
      - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    )
    ELSE 0
  END AS avg_order_cents,
  COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS ordering_sessions
FROM deduped
GROUP BY shop_id, tenant_id, day;

CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_orders ON silver.daily_orders (shop_id, day);


-- Воронка за день: шаги визита — только люди; «покупки» — визиты с покупкой, это заказы (деньги),
-- поэтому без фильтра по пометке — как заказы в дашборде. Формулы — из 004.
CREATE MATERIALIZED VIEW silver.daily_funnel AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', event_timestamp)::date AS day,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view', 'session_start')) AS visits,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'product_view') AS product_views,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'checkout_start') AS checkout_starts,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS purchases
FROM bronze.events
WHERE (traffic_type = 'human' OR event_type IN ('purchase', 'order_cancel'))
  AND event_timestamp >= now() - interval '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', event_timestamp)::date;

CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_funnel ON silver.daily_funnel (shop_id, day);


-- Каналы за день: сессии — начала сессий людей; заказы и выручка — события покупки, все.
-- Формулы — из 004.
CREATE MATERIALIZED VIEW silver.daily_channel_attribution AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', event_timestamp)::date AS day,
    COALESCE(utm_source, 'direct') AS channel,
    utm_medium,
    utm_campaign,
    COUNT(*) FILTER (WHERE event_type = 'session_start') AS sessions,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS orders,
    SUM(order_total_cents) FILTER (WHERE event_type = 'purchase') AS revenue_cents
FROM bronze.events
WHERE (traffic_type = 'human' OR event_type IN ('purchase', 'order_cancel'))
  AND event_timestamp >= now() - interval '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', event_timestamp)::date,
         COALESCE(utm_source, 'direct'), utm_medium, utm_campaign;

-- Колонки без выражений + NULLS NOT DISTINCT: годится для CONCURRENTLY (013).
CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_channel_attribution ON silver.daily_channel_attribution
    (shop_id, day, channel, utm_medium, utm_campaign) NULLS NOT DISTINCT;


-- «Сессии по локациям» за день: визиты людей по гео и чистые заказы по гео визита.
-- Устройство — из 015: гео визита берётся из его просмотров (теперь — только людей); заказ
-- относится к гео своего визита; заказ без визита человека (сервис заказов, робот) — в
-- «Не определено» (NULL), а не теряется. Деньги — все.
CREATE MATERIALIZED VIEW silver.daily_geo AS
WITH
session_geo AS (
    SELECT DISTINCT ON (shop_id, session_id)
        shop_id,
        session_id,
        geo_country,
        geo_subject,
        geo_city
    FROM bronze.events
    WHERE event_type IN ('page_view', 'session_start')
      AND traffic_type = 'human'
      AND event_timestamp >= now() - interval '13 months'
    ORDER BY shop_id, session_id,
             (geo_country IS NULL),   -- located events first
             event_timestamp          -- then first touch
),
session_days AS (
    SELECT DISTINCT
        shop_id,
        session_id,
        date_trunc('day', event_timestamp)::date AS day
    FROM bronze.events
    WHERE event_type IN ('page_view', 'session_start')
      AND traffic_type = 'human'
      AND event_timestamp >= now() - interval '13 months'
),
sessions_agg AS (
    SELECT
        sd.shop_id,
        sd.day,
        sg.geo_country,
        sg.geo_subject,
        sg.geo_city,
        COUNT(*) AS sessions
    FROM session_days sd
    JOIN session_geo sg
      ON sg.shop_id = sd.shop_id AND sg.session_id = sd.session_id
    GROUP BY sd.shop_id, sd.day,
             sg.geo_country, sg.geo_subject, sg.geo_city
),
valid_orders AS (
    SELECT DISTINCT shop_id, order_id
    FROM bronze.events
    WHERE event_type = 'purchase' AND order_id IS NOT NULL
),
order_events AS (
    SELECT
        e.shop_id,
        date_trunc('day', e.event_timestamp)::date AS day,
        e.order_id,
        e.event_type,
        MAX(e.order_total_cents) AS order_total_cents,
        MAX(e.session_id)        AS session_id
    FROM bronze.events e
    WHERE e.event_type IN ('purchase', 'order_cancel')
      AND e.order_id IS NOT NULL
      AND e.event_timestamp >= now() - interval '13 months'
      AND (
        e.event_type = 'purchase'
        OR EXISTS (
          SELECT 1 FROM valid_orders v
          WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
        )
      )
    GROUP BY e.shop_id, date_trunc('day', e.event_timestamp)::date,
             e.order_id, e.event_type
),
orders_agg AS (
    SELECT
        oe.shop_id,
        oe.day,
        sg.geo_country,
        sg.geo_subject,
        sg.geo_city,
        COUNT(*) FILTER (WHERE oe.event_type = 'purchase')
          - COUNT(*) FILTER (WHERE oe.event_type = 'order_cancel') AS orders,
        COALESCE(SUM(oe.order_total_cents) FILTER (WHERE oe.event_type = 'purchase'), 0)
          - COALESCE(SUM(oe.order_total_cents) FILTER (WHERE oe.event_type = 'order_cancel'), 0)
          AS revenue_cents
    FROM order_events oe
    LEFT JOIN session_geo sg
      ON sg.shop_id = oe.shop_id AND sg.session_id = oe.session_id
    GROUP BY oe.shop_id, oe.day,
             sg.geo_country, sg.geo_subject, sg.geo_city
)
SELECT
    COALESCE(s.shop_id, o.shop_id)         AS shop_id,
    COALESCE(s.day, o.day)                 AS day,
    COALESCE(s.geo_country, o.geo_country) AS geo_country,
    COALESCE(s.geo_subject, o.geo_subject) AS geo_subject,
    COALESCE(s.geo_city, o.geo_city)       AS geo_city,
    COALESCE(s.sessions, 0)      AS sessions,
    COALESCE(o.orders, 0)        AS orders,
    COALESCE(o.revenue_cents, 0) AS revenue_cents
FROM sessions_agg s
FULL OUTER JOIN orders_agg o
  ON  s.shop_id = o.shop_id
  AND s.day = o.day
  AND s.geo_country IS NOT DISTINCT FROM o.geo_country
  AND s.geo_subject IS NOT DISTINCT FROM o.geo_subject
  AND s.geo_city    IS NOT DISTINCT FROM o.geo_city;

CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_geo
    ON silver.daily_geo (shop_id, day, geo_country, geo_subject, geo_city) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_daily_geo_day ON silver.daily_geo (day);


-- Сутки магазина для дашборда: трафик людей + деньги. Формулы — из 012/014.
-- FULL JOIN вместо LEFT JOIN: раньше трафик считался по всем событиям, и день с заказом всегда
-- был в daily_traffic (сама покупка давала строку). Теперь трафик — только люди, и день, где был
-- только заказ (покупку прислал сервис заказов с подписью робота, а визит был у робота или своих),
-- при LEFT JOIN потерял бы выручку. Деньги не теряются.
CREATE MATERIALIZED VIEW gold.dashboard_kpi AS
SELECT
    COALESCE(t.shop_id, o.shop_id)     AS shop_id,
    COALESCE(t.tenant_id, o.tenant_id) AS tenant_id,
    COALESCE(t.day, o.day)             AS day,
    COALESCE(t.page_views, 0)          AS page_views,
    COALESCE(t.unique_sessions, 0)     AS unique_sessions,
    COALESCE(t.unique_visitors, 0)     AS unique_visitors,
    COALESCE(o.order_count, 0)         AS order_count,
    COALESCE(o.total_revenue_cents, 0) AS total_revenue_cents,
    COALESCE(o.avg_order_cents, 0)     AS avg_order_cents,
    CASE WHEN t.unique_sessions > 0
        THEN ROUND(COALESCE(o.order_count, 0)::numeric / t.unique_sessions * 100, 2)
        ELSE 0 END AS conversion_rate
FROM silver.daily_traffic t
FULL JOIN silver.daily_orders o ON t.shop_id = o.shop_id AND t.day = o.day;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dashboard_kpi ON gold.dashboard_kpi (shop_id, day);


-- Топ товаров за последние 30 дней (окно прежнее, теперь по времени покупки) — деньги, все.
-- Отмена вычитается, только если покупка того же заказа — в том же окне (014): иначе товар
-- уходил бы в минус. Сумма позиций заказа по товару — 011.
CREATE MATERIALIZED VIEW gold.top_products AS
WITH valid_orders AS (
  SELECT DISTINCT shop_id, order_id
  FROM bronze.events
  WHERE event_type = 'purchase' AND order_id IS NOT NULL
    AND event_timestamp >= now() - interval '30 days'
),
deduped_products AS (
  SELECT
    e.shop_id, e.tenant_id, e.product_id, e.order_id, e.event_type,
    MAX(e.product_name) AS product_name,
    SUM(e.product_price_cents) AS product_price_cents
  FROM bronze.events e
  WHERE e.event_type IN ('purchase', 'order_cancel')
    AND e.product_id IS NOT NULL
    AND e.event_timestamp >= now() - interval '30 days'
    AND (
      e.event_type = 'purchase'
      OR EXISTS (
        SELECT 1 FROM valid_orders v
        WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
      )
    )
  GROUP BY e.shop_id, e.tenant_id, e.product_id, e.order_id, e.event_type
)
SELECT
  shop_id, tenant_id, product_id,
  MAX(product_name) AS product_name,
  COUNT(*) FILTER (WHERE event_type = 'purchase')
    - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    AS sales_count,
  COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'purchase'), 0)
    - COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    AS total_revenue_cents
FROM deduped_products
GROUP BY shop_id, tenant_id, product_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_top_products ON gold.top_products (shop_id, product_id);

COMMIT;
