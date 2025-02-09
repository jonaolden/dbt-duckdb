with source as (select * from {{ source("dlt", "kolada_data") }})


select * from source 