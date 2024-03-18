{{
    config(
        materialized='incremental',
        unique_key='global_id',
        schema='dds'
    )
}}

select t.global_id, t.year, t.transport_type, t.passenger_traffic, {{ dbt_date.now("Europe/Moscow") }} as on_date
from stg.tbl_transport t
where t.id = (select max(tt.id) as id from stg.tbl_transport tt where tt.global_id = t.global_id)
group by t.global_id, t.year, t.transport_type, t.passenger_traffic