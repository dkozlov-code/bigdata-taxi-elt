select
    idempotency_key,
    cast(pickup_datetime as date) as service_date,
    vendor_id,
    {{ region('pickup_latitude', 'pickup_longitude') }} as region,
    cast(fare_amount as decimal(18, 2)) as fare_usd,
    trip_distance
from {{ source('raw', 'trips') }}
where fare_amount >= 0 and trip_distance > 0
