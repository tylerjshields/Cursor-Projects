with exposures as (
    select try_to_number(bucket_key) consumer_id, experiment_group, first_exposure_time
    from metrics_repo.public.schnucks_isp_test_exposures 
), 
spendd as (
    select delivery_address_id, event_date, sum(cng_pre_inflated_subtotal) spend
    -- from proddb.tyleranderson.store_impressions_dedupe
    from metrics_repo.public.cng_deliveries 
    group by all
    -- limit 10
)
select 
event_ts::DATE as event_date,
experiment_group,
count(distinct e.consumer_id) num_exposed,
sum(spend) spend
from exposures e
left join spendd s on e.consumer_id = s.consumer_id and s.timestamp >= e.first_exposure_time 
group by all

-- limit 10
