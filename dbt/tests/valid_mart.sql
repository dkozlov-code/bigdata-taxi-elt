select * from {{ ref('daily_revenue') }}
where revenue_usd < 0 or trips < 1
