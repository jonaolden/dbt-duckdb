with source as (select * from {{ source('kolada', 'kolada_municipalities') }})


,renamed as (

    select	
    municipality_id,
	municipality_name,
	municipality_type,
	_dlt_load_id,
	_dlt_id from source
  )

  select * from renamed 