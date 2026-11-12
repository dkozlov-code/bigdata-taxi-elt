select
    t.service_date,
    t.region,
    t.vendor_id,
    v.vendor_name,
    count(*) as trips,
    sum(t.fare_usd) as revenue_usd,
    sum(t.trip_distance) as distance_miles
from {{ ref('stg_trips') }} t
join {{ ref('dim_vendors') }} v using (vendor_id)
group by all
