{{
    config(materialized='table',
    file_format='delta',
    tag='gold_layer',
    partition_by='year',
    clustered_by='sector',
    incremental_strategy='insert_overwrite')

}}

select
    sector                                      as sector,
    extract(year from end_date)                 as year,
    substring(gas,1,3)                          as gas,
    round(sum(emissions_quantity)/1000000,2)    as megatonne_emissions_quantity
from {{ref('overall_emission_data')}}
group by 1,2,3
order by 1,2,3