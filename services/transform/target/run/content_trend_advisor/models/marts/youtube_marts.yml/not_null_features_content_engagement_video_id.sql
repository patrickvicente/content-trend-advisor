select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select video_id
from "contentdb"."public_marts"."features_content_engagement"
where video_id is null



      
    ) dbt_internal_test