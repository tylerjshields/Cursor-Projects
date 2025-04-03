--DSFE
create or replace temporary table temp_date_series as
select dateadd('day', seq4(), '2025-01-31'::date) as date
from table(generator(rowcount => 54));

with exposures as (
    select bucket_key, experiment_group, first_exposure_time
    from metrics_repo.public.schucks_inflation_winsorized_p997_exposures 
), 
spendd as (
    select delivery_address_id, event_ts, count(1) spend
    from metrics_repo.public.cng_deliveries d
    join edw.cng.dimension_new_vertical_store_tags nv on d.store_id = nv.store_id
    where d.business_id = 13625077
    group by all
),
user_dates as (
    select 
        e.bucket_key,
        e.experiment_group,
        e.first_exposure_time,
        d.date,
        datediff('d', e.first_exposure_time, d.date) as days_since_first_exposure
    from exposures e
    cross join temp_date_series d
    where d.date >= e.first_exposure_time::date
),
daily_spend as (
    select 
        ud.days_since_first_exposure,
        ud.experiment_group,
        ud.bucket_key,
        coalesce(sum(s.spend), 0) as daily_spend
    from user_dates ud
    left join spendd s on ud.bucket_key = s.delivery_address_id 
        and ud.date = s.event_ts::date
    group by 1, 2, 3
),
cumulative_spend as (
    select 
        days_since_first_exposure,
        experiment_group,
        count(distinct bucket_key) as num_exposed,
        sum(daily_spend) as total_spend,
        sum(daily_spend) / nullif(count(distinct bucket_key), 0) as spend_per_exposed,
        sum(sum(daily_spend)) over (
            partition by experiment_group 
            order by days_since_first_exposure 
            rows between unbounded preceding and current row
        ) as cumulative_spend,
        sum(sum(daily_spend)) over (
            partition by experiment_group 
            order by days_since_first_exposure 
            rows between unbounded preceding and current row
        ) / nullif(count(distinct bucket_key), 0) as cumulative_spend_per_exposed
    from daily_spend
    group by 1, 2
),
treatment_control_comparison as (
    select 
        days_since_first_exposure,
        experiment_group,
        num_exposed,
        total_spend,
        spend_per_exposed,
        cumulative_spend,
        cumulative_spend_per_exposed,
        -- Calculate relative difference from control group
        (spend_per_exposed - first_value(spend_per_exposed) over (
            partition by days_since_first_exposure 
            order by case when experiment_group = 'control' then 0 else 1 end
            rows between unbounded preceding and unbounded following
        )) / nullif(first_value(spend_per_exposed) over (
            partition by days_since_first_exposure 
            order by case when experiment_group = 'control' then 0 else 1 end
            rows between unbounded preceding and unbounded following
        ), 0) as spend_per_exposed_rel_diff,
        -- Calculate relative difference in cumulative spend
        (cumulative_spend_per_exposed - first_value(cumulative_spend_per_exposed) over (
            partition by days_since_first_exposure 
            order by case when experiment_group = 'control' then 0 else 1 end
            rows between unbounded preceding and unbounded following
        )) / nullif(first_value(cumulative_spend_per_exposed) over (
            partition by days_since_first_exposure 
            order by case when experiment_group = 'control' then 0 else 1 end
            rows between unbounded preceding and unbounded following
        ), 0) as cumulative_spend_rel_diff
    from cumulative_spend
)
select 
    days_since_first_exposure,
    experiment_group,
    num_exposed,
    total_spend,
    spend_per_exposed,
    cumulative_spend,
    cumulative_spend_per_exposed,
    spend_per_exposed_rel_diff,
    cumulative_spend_rel_diff,
    -- Calculate day-over-day growth in spend per exposed
    (spend_per_exposed - lag(spend_per_exposed) over (
        partition by experiment_group 
        order by days_since_first_exposure
    )) / nullif(lag(spend_per_exposed) over (
        partition by experiment_group 
        order by days_since_first_exposure
    ), 0) as spend_per_exposed_growth
from treatment_control_comparison
where days_since_first_exposure >= 0  -- Only look at post-exposure days
order by experiment_group, days_since_first_exposure;

with exposures as (
    select bucket_key, experiment_group, first_exposure_time::DATE dt
    from metrics_repo.public.schucks_inflation_winsorized_p997_exposures 
)
select experiment_group, dt, count(1) num_exposures
