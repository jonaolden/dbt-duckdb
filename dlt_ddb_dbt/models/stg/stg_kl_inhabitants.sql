with source as (select * from {{ source('kolada', 'kolada_inhabitants') }})


,renamed as (
select
	kpi,
	municipality,
	period,
	count,
	gender,
	status,
	value,
	municipality_id,
	_dlt_load_id,
	_dlt_id
    from source)

select * from renamed