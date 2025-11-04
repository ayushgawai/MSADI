{% snapshot snapshot_session_summary %}
{{
  config(
    target_schema='SNAPSHOTS',
    unique_key='sessionid',
    strategy='check',
    check_cols=['USERID', 'CHANNEL', 'TS']
  )
}}
select
    sessionid,
    userid,
    channel,
    ts
from {{ ref('session_summary') }}
{% endsnapshot %}
