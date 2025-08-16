{{ config(schema='business') }}

select
    'RELIANCE'  symbol,
    date_trunc(month, date) as month,
    avg(open) as avg_open,
    avg(close) as avg_close,
    avg(volume) as avg_volume
from {{ ref('raw_nse_bhavcopy') }}
group by symbol, date_trunc(month, date)
order by symbol, month
