{{ config(materialized='view') }}

select * from {{ ref('UsedProducts_NAM') }}
union all
select * from {{ ref('UsedProducts_EMEA') }}
union all
select * from {{ ref('UsedProducts_APAC') }}