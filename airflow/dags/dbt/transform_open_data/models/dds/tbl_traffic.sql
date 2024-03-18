{{
    config(
        materialized='table',
        schema='dds'
    )
}}

select t.year, t.transport_type, t.passenger_traffic
from {{ ref("tbl_transport") }} t
where t.on_date >= (select max(on_date) as on_date from ods.tbl_transport tt)
group by t.year, t.transport_type, t.passenger_traffic
