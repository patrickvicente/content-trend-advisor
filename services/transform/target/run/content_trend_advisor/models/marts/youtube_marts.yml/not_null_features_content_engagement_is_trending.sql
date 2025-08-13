select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select is_trending
from "contentdb"."public_marts"."features_content_engagement"
where is_trending is null



      
    ) dbt_internal_test