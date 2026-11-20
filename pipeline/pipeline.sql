-- stage: deduplicate and validate events
CREATE TABLE events AS
WITH winners AS (
    -- Mark bad payloads with a null timestamp while retaining their event IDs
    -- and ingest time so that a later valid duplicate cannot replace them.
    SELECT arg_min(
        struct_pack(
            user_id := user_id,
            ts := CASE WHEN product_id IS NOT NULL AND (
                event_type IN ('view', 'cart')
                OR (event_type = 'purchase' AND price_cents BETWEEN 1 AND 10000000)
            ) THEN ts END,
            bought_product := CASE WHEN event_type = 'purchase' THEN product_id END,
            price := price_cents,
            ingest_ts := ingest_ts
        ),
        ingest_ts
    ) AS event
    FROM raw_events
    WHERE event_id IS NOT NULL
    GROUP BY event_id
), parsed AS (
    SELECT event.user_id AS user_id, event.ts AS ts, event.ingest_ts AS ingest_ts,
           event.bought_product AS bought_product, event.price AS price
    FROM winners
    WHERE event.user_id IS NOT NULL
)
SELECT coalesce(i.person, p.user_id) AS person, p.ts, p.bought_product, p.price
FROM parsed p
CROSS JOIN bounds b
LEFT JOIN identities i USING (user_id)
WHERE p.ts >= b.start_ms AND p.ts < b.end_ms
    AND p.ts >= p.ingest_ts - 86400000;

-- stage: sessionise and aggregate events
-- RANGE includes all events at the current timestamp, including peers of the
-- row that starts a session in the lag window.
CREATE TABLE session_categories AS
WITH gaps AS (
    SELECT *, ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) AS gap
    FROM events
), sessionised AS (
    SELECT person, bought_product, price,
           max(CASE WHEN gap IS NULL OR gap > 1800000 THEN ts END)
               OVER (PARTITION BY person ORDER BY ts RANGE UNBOUNDED PRECEDING) AS start_ms
    FROM gaps
)
SELECT s.person, s.start_ms,
       CASE WHEN s.bought_product IS NOT NULL THEN coalesce(p.category, 'unknown') END AS category,
       count(*) AS events, coalesce(sum(s.price), 0)::BIGINT AS revenue_cents
FROM sessionised s
LEFT JOIN raw_products p ON s.bought_product = p.product_id
GROUP BY ALL;
DROP TABLE events;

CREATE TABLE sessions AS
SELECT person, start_ms, make_timestamp_ms(start_ms)::DATE AS day,
       sum(events)::BIGINT AS events, sum(revenue_cents)::BIGINT AS revenue_cents
FROM session_categories
GROUP BY person, start_ms;

-- stage: aggregate user days
CREATE TABLE user_daily AS
WITH category_revenue AS (
    SELECT person, make_timestamp_ms(start_ms)::DATE AS day, category,
           sum(revenue_cents) AS revenue
    FROM session_categories
    WHERE category IS NOT NULL
    GROUP BY ALL
), top_categories AS (
    SELECT person, day, first(category ORDER BY revenue DESC, category) AS top_category
    FROM category_revenue
    GROUP BY person, day
), totals AS (
    SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions
    GROUP BY person, day
)
SELECT person AS user_id, day, sessions, events, revenue_cents, top_category
FROM totals
LEFT JOIN top_categories USING (person, day);
DROP TABLE session_categories;

-- stage: deduplicate and validate clicks
CREATE TABLE clicks AS
WITH unique_clicks AS (
    SELECT DISTINCT click_id, user_id, campaign_id, ts
    FROM raw_clicks
    WHERE click_id IS NOT NULL AND user_id IS NOT NULL
)
SELECT coalesce(i.person, p.user_id) AS person, p.click_id, p.campaign_id, p.ts
FROM unique_clicks p
JOIN raw_campaigns c USING (campaign_id)
LEFT JOIN identities i USING (user_id)
CROSS JOIN bounds b
WHERE p.ts >= b.start_ms - 604800000 AND p.ts < b.end_ms;

-- stage: attribute last click
CREATE TABLE credits AS
WITH winning_clicks AS (
    SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id
    FROM clicks
    GROUP BY person, ts
)
SELECT CASE WHEN c.ts >= s.start_ms - 604800000 THEN c.campaign_id ELSE 'organic' END AS campaign_id,
       s.day, count(*) AS sessions,
       count(*) FILTER (WHERE s.revenue_cents > 0) AS purchasing_sessions,
       sum(s.revenue_cents)::BIGINT AS revenue_cents
FROM sessions s
ASOF LEFT JOIN winning_clicks c ON s.person = c.person AND s.start_ms >= c.ts
GROUP BY 1, 2;

-- stage: count campaign assists
-- Assign an assisted session to the latest eligible click of its campaign
-- so same-campaign intervals are disjoint. Count sessions with two ASOF
-- lookups on the daily prefix, without creating one row per session/click pair.
CREATE TABLE assists AS
WITH unique_times AS (
    SELECT DISTINCT person, campaign_id, ts FROM clicks
), intervals AS (
    SELECT person, campaign_id, ts AS first_ms,
           least(ts + 604800000, lead(ts) OVER (
               PARTITION BY person, campaign_id ORDER BY ts
           ) - 1) AS last_ms
    FROM unique_times
), per_day AS (
    SELECT c.person, c.campaign_id, u.day,
           greatest(c.first_ms, epoch_ms(u.day)) AS first_ms,
           least(c.last_ms, epoch_ms(u.day) + 86399999) AS last_ms
    FROM intervals c
    JOIN user_daily u ON u.user_id = c.person
        AND c.first_ms < epoch_ms(u.day) + 86400000 AND c.last_ms >= epoch_ms(u.day)
), prefix AS MATERIALIZED (
    SELECT person, day, start_ms,
           row_number() OVER (PARTITION BY person, day ORDER BY start_ms) AS n
    FROM sessions
), ends AS (
    SELECT d.*, e.n AS end_n
    FROM per_day d
    ASOF JOIN prefix e ON d.person = e.person AND d.day = e.day AND d.last_ms >= e.start_ms
)
SELECT e.campaign_id, e.day, sum(e.end_n - coalesce(b.n, 0))::BIGINT AS assisted_sessions
FROM ends e
ASOF LEFT JOIN prefix b ON e.person = b.person AND e.day = b.day AND e.first_ms > b.start_ms
GROUP BY e.campaign_id, e.day
HAVING assisted_sessions > 0;

-- stage: aggregate campaign days
CREATE TABLE campaign_daily AS
SELECT campaign_id, CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE channel END AS channel,
       day, coalesce(sessions, 0) AS sessions, coalesce(purchasing_sessions, 0) AS purchasing_sessions,
       coalesce(revenue_cents, 0) AS revenue_cents, coalesce(assisted_sessions, 0) AS assisted_sessions
FROM credits
FULL JOIN assists USING (campaign_id, day)
LEFT JOIN raw_campaigns USING (campaign_id);
