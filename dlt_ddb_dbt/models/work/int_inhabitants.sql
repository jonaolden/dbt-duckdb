with source as (select * from {{ ref('stg_kl_inhabitants') }})

,final as ( 
select 	
    municipality_id,
	period,
    gender, 
  	value as inhabitants 
    from source
)

   select * from final  