{{
    config(materialized='table',
    file_format='delta',
    tag='silver_layer',
    incremental_strategy='insert_overwrite')

}}

with agriculture as (

    select
    cast('agriculture'              as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('agriculture')}}

),

buildings as (

    select
    cast('buildings'                as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('buildings')}}

),

fluorinated_gases as (

    select
    cast('fluorinated_gases'        as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('fluorinated_gases')}}
),

fossil_fuel_operations as (

    select
    cast('fossil_fuel_operations'   as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('fossil_fuel_operations')}}

),

manufacturing as (

    select
    cast('manufacturing'            as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('manufacturing')}}

),

mineral_extraction as (

    select
    cast('mineral_extraction'       as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('mineral_extraction')}}

),

power as (

    select
    cast('power'                    as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('power')}}

),

transportation as (

    select
    cast('transportation'           as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('transportation')}}

),

waste as (

    select
    cast('waste'                    as string)  as sector,                 
    cast(iso3_country               as string)  as country,
    cast(start_time                 as date)    as start_date,
    cast(end_time                   as date)    as end_date,
    cast(original_inventory_sector  as string)  as original_inventory_sector,
    cast(gas                        as string)  as gas,
    cast(emissions_quantity         as double)  as emissions_quantity,
    cast(emissions_quantity_units   as string)  as emissions_quantity_units,
    cast(created_date               as date)    as created_date
    
    from {{ref('waste')}}

)

select * from agriculture
union all
select * from buildings
union all
select * from fluorinated_gases
union all
select * from fossil_fuel_operations
union all
select * from manufacturing
union all
select * from mineral_extraction
union all
select * from power
union all
select * from transportation
union all
select * from waste
