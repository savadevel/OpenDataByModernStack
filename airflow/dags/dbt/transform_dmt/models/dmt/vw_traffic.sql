{{
    config(
        materialized='view'
    )
}}

select t.year, t.transport_type, t.passenger_traffic
from {{ source('open_data', 'dwh_dds_traffic') }} t
