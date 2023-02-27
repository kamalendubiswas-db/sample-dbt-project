{{
    config(materialized='table',
    file_format='delta',
    tag='bronze_layer',
    incremental_strategy='insert_overwrite')

}}

select * from {{ source('raw_layer', 'raw_manufacturing') }}