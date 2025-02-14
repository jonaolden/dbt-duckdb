with source as (select * from {{ source('kolada', 'kolada_hospital_beds') }})

,renamed as (
      select
    kpi,
	municipality,
	period,
	count,
	gender,
	status,
	value,
	year,
	_dlt_load_id,
	_dlt_id

      from source
  )
  select * from renamed
    