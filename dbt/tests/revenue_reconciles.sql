-- Не теряем выручку при join и агрегации. Пустой результат означает успех.
select 'revenue_mismatch' as error
where (select coalesce(sum(revenue_usd), 0) from {{ ref('daily_revenue') }})
   <> (select coalesce(sum(fare_usd), 0) from {{ ref('stg_trips') }})
