
    
    

select
    video_id as unique_field,
    count(*) as n_records

from "contentdb"."public_marts"."features_content_engagement"
where video_id is not null
group by video_id
having count(*) > 1


