with source as (select * from {{ ref('stg_kl_available_hospital_beds') }}) 

,final as (select

	municipality as municipality_id,
	coalesce(year,period) as period,
  	value as available_beds_per_1k_population


from source)


select * from final
