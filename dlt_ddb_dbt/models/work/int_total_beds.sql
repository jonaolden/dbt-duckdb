with source_beds as (select * from {{ ref('int_available_hospital_beds') }}) 
,source_inhabitants as (select * from {{ ref('int_inhabitants') }})


,get_total_population as (select sum(inhabitants) as total_population, municipality_id, period 
from source_inhabitants group by all )

,get_beds_normalized as (select municipality_id, period, available_beds_per_1k_population 
from source_beds)

,get_total_beds as (select a.municipality_id, a.period, (a.available_beds_per_1k_population * b.total_population * 1000) as total_bed_count, b.total_population
from get_beds_normalized a left join get_total_population b on a.municipality_id = b.municipality_id and a.period = b.period)

,final as (select * from get_total_beds)


select * from final 