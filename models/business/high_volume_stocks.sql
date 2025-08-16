{{ config(schema='business') }}
select
    date,
	'RELIANCE' symbol,
    volume
from {{ ref('raw_nse_bhavcopy') }}
where date = (select max(date) from {{ ref('raw_nse_bhavcopy') }})
order by volume desc
limit 10
