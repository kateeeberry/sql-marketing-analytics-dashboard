
WITH combined_data AS (
    -- Step 1A: Get new account registrations by date and market
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
    GROUP BY 1,2,3,4,5

    UNION ALL

    -- Step 1B: Get email interaction funnel (Sent -> Open -> Visit)
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
    GROUP BY 1,2,3,4,5
),

-- Step 2: Post-Union Aggregation
-- Aggregating metrics at the country/date level to ensure data integrity
-- and eliminate duplicate rows generated during the UNION ALL process.
final_data AS (
    SELECT 
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,
        SUM(sent_msg) AS sent_msg,
        SUM(open_msg) AS open_msg,
        SUM(visit_msg) AS visit_msg
    FROM combined_data
    GROUP BY 1,2,3,4,5
),

-- Step 3: Global Ranking Logic
-- Identifying Top 10 markets by total accounts and total sent messages 
-- using Window Functions to enable filtering of the global long-tail.
ranked_data AS (
    SELECT *,
      DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
      DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM (
      SELECT *,
        SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
      FROM final_data 
    )
)

-- Step 4: Final Normalization (UNPIVOT)
-- Transforming metrics from columns to rows to allow dynamic metric selection 
-- and efficient data handling in the Looker Studio dashboard.
SELECT
    *
FROM ranked_data
UNPIVOT(
    metric_value FOR metric_name IN (account_cnt, sent_msg, open_msg, visit_msg)
)
WHERE rank_total_country_account_cnt <= 10
   OR rank_total_country_sent_cnt <= 10;
