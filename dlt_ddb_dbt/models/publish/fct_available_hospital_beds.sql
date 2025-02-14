with source_mun as (select * from {{ ref('int_municipality') }})
,source_beds as (select * from {{ ref('int_available_hospital_beds') }})


,join_tables as (

select

    b.municipality_id,
	b.municipality_name,
	b.municipality_type,
    a.period,
    a.total_bed_count,
    a.total_population
     from source_beds a left join source_mun b on a.municipality_id = b.municipality_id


)

,final as (select * from join_tables)

select * from final