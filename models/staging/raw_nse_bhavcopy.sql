{{ config(
    materialized = 'view',
    schema = 'staging'
) }}

select
    date,
    open,
    high,
    low,
    close,
    adj_close,
    volume
from {{ source('raw', 'raw_nse_bhavcopy') }}
