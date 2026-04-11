
-- STEP 1: Combine subscriber data and email activity metrics
WITH combined_data AS (
    -- Sub-step A: Aggregate Account Registrations by Date and Country
    SELECT
        s.date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT(DISTINCT ac.id) AS account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg
    FROM `DA.account` ac
    JOIN `DA.account_session` acs ON acs.account_id = ac.id
    JOIN `DA.session_params` sp ON sp.ga_session_id = acs.ga_session_id
    JOIN `DA.session` s ON s.ga_session_id = sp.ga_session_id
    GROUP BY 1, 2, 3, 4, 5

    UNION ALL

    -- Sub-step B: Aggregate Email Engagement (Sent, Open, Visit)
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        0 AS account_cnt,
        COUNT(es.id_message) AS sent_msg,
        COUNT(eo.id_message) AS open_msg,
        COUNT(ev.id_message) AS visit_msg
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` eo ON eo.id_message = es.id_message
    LEFT JOIN `DA.email_visit` ev ON ev.id_message = es.id_message
    JOIN `DA.account` ac ON ac.id = es.id_account
    JOIN `DA.account_session` acs ON acs.account_id = es.id_account
    JOIN `DA.session` s ON s.ga_session_id = acs.ga_session_id
    JOIN `DA.session_params` sp ON sp.ga_session_id = s.ga_session_id
    GROUP BY 1, 2, 3, 4, 5
),

-- STEP 2: Calculate Global Market Ranking and Totals
final_data AS (
    SELECT *,
        -- Calculate rank based on total accounts per country
        DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        -- Calculate rank based on total emails sent per country
        DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM (
        SELECT *,
            -- Sum metrics at the country level using Window Functions
            SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
            SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
        FROM combined_data
    )
)

-- STEP 3: Normalize data for BI visualization and filter Top 10 markets
SELECT 
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt,
    metric_name,
    metric_value
FROM final_data
-- Pivot metrics from columns to rows for flexible filtering in Looker Studio
UNPIVOT(
    metric_value FOR metric_name IN (account_cnt, sent_msg, open_msg, visit_msg)
)
-- Filter to include only top-performing countries to reduce noise in charts
WHERE rank_total_country_account_cnt <= 10 
   OR rank_total_country_sent_cnt <= 10;
