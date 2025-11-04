{{ config(
    materialized = 'table',
    unique_key = ['sessionid'],
) }}

SELECT
    sessionid,
    ts
FROM
    {{ source(
        'raw',
        'session_timestamp'
    ) }}
