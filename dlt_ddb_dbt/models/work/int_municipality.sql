with source as (select * from {{ ref('stg_kl_municipality') }}) 


  ,final as (

    select	
    municipality_id,
	municipality_name,
	municipality_type from source


  )

  select * from final 