{{ config(materialized='view') }}

select * from {{ ref('Consumers_NAM') }}
union all
select * from {{ ref('Consumers_EMEA') }}
union all
select * from {{ ref('Consumers_APAC') }}