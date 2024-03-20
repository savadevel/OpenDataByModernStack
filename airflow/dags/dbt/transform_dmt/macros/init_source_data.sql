{% macro init_source_data() -%}

    {% set sql %}

        CREATE TABLE IF NOT EXISTS open_data.dwh_dds_traffic
        (
            year Int16,
            transport_type String,
            passenger_traffic Int64
        ) ENGINE = PostgreSQL('dwh:5432', 'open_data', 'tbl_traffic', 'user', 'pass', 'dds');

    {% endset %}
    
    {% set result = run_query(sql) %}

    {{ print('Initialized DWH source database') }}

{%- endmacro %}
