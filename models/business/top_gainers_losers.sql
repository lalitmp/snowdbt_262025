{{ config(schema='business') }}

with daily_change as (
    select 
        date,
        'RELIANCE'  symbol,
        close,
        lag(close) over (partition by symbol order by date) as prev_close,
        round(
            ((close - lag(close) over (partition by 'RELIANCE'  order by date)) / nullif(lag(close) over (partition by 'RELIANCE'  order by date), 0)) * 100, 
            2
        ) as pct_change
    from {{ ref('raw_nse_bhavcopy') }}
)
select *
from daily_change
where date = (select max(date) from {{ ref('raw_nse_bhavcopy') }})
order by pct_change desc
limit 10
