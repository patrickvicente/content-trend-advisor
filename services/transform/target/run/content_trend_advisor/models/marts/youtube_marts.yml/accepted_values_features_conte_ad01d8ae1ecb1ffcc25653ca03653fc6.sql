select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        engagement_tier as value_field,
        count(*) as n_records

    from "contentdb"."public_marts"."features_content_engagement"
    group by engagement_tier

)

select *
from all_values
where value_field not in (
    'high','medium','low','very_low'
)



      
    ) dbt_internal_test