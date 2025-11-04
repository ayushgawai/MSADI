{{ config(
    materialized = 'table',
    unique_key = ['sessionid'],
) }}
WITH user_dedup AS (

    SELECT
        sessionid,
        MIN(userid) AS userid,
        MIN(channel) AS channel
    FROM {{ ref('user_session_channel') }}
    GROUP BY sessionid
),
ts_dedup AS (
    SELECT
        sessionid,
        MAX(ts) AS ts
    FROM {{ ref('session_timestamp') }}
    GROUP BY sessionid
),
joined AS (
    SELECT
        u.sessionid,
        u.userid,
        u.channel,
        t.ts
    FROM
        user_dedup u
        INNER JOIN ts_dedup t
        ON u.sessionid = t.sessionid
)
SELECT
    *
FROM
    joined qualify ROW_NUMBER() over (
        PARTITION BY sessionid
        ORDER BY
            ts DESC
    ) = 1
