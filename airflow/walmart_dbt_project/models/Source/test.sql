
select * from {{ source('walmart_databricks', 'orders') }}