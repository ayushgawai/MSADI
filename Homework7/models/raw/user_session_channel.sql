{{ config(
    materialized = 'table',
    unique_key = ['sessionid'],
) }}

SELECT
    sessionid,
    userid,
    channel
FROM
    {{ source(
        'raw',
        'user_session_channel'
    ) }}
