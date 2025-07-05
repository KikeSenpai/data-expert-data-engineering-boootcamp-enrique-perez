-- Initial population of user_devices_cumulated table
insert into user_devices_cumulated
with initial_events as (
    select
        user_id,
        device_id,
        cast(event_time as date) as event_date
    from events
    where
        true
        and user_id is not null
        and cast(event_time as date) = '2023-01-01'
),

enriched_events as (
    select
        e.user_id,
        d.browser_type,
        array_agg(distinct e.event_date) as active_datelist
    from initial_events as e
    left join devices as d
        on e.device_id = d.device_id
    group by 1, 2
),

final_cte as (
    select
        user_id,
        array_agg((browser_type, active_datelist)::browser_datelist) as device_activity_datelist
    from enriched_events
    group by 1
)

select
    user_id,
    device_activity_datelist,
    '2023-01-01'::date as last_updated_at
from final_cte;


-- Cumulative update for user devices with active datelists
insert into user_devices_cumulated
with yesterday_events as (
    select
        user_id,
        device_id,
        cast(event_time as date) as event_date
    from events
    where
        true
        and user_id is not null
        and cast(event_time as date) = '2023-01-02'
),

enriched_events as (
    select
        e.user_id,
        d.browser_type,
        array_agg(distinct e.event_date) as active_datelist
    from yesterday_events as e
    left join devices as d
        on e.device_id = d.device_id
    group by 1, 2
),

current_users as (
    select
        user_id,
        last_updated_at,
        (browser_datelist).browser_type,
        (browser_datelist).datelist as active_datelist
    from
        user_devices_cumulated,
        unnest(device_activity_datelist) as browser_datelist
),

joined_data as (
    select
        coalesce(c.user_id, e.user_id) as user_id,
        coalesce(c.browser_type, e.browser_type) as browser_type,
        coalesce(
            c.active_datelist,
            array_fill(
                null::date,
                array['2023-01-02'::date - '2023-01-01'::date]
            )
        ) || coalesce(e.active_datelist, array[null::date]) as active_datelist,
        '2023-01-02'::date as last_updated_at
    from current_users as c
    full outer join enriched_events as e
        on c.user_id = e.user_id
),

final_cte as (
    select
        user_id,
        array_agg((browser_type, active_datelist)::browser_datelist) as device_activity_datelist,
        max(last_updated_at) as last_updated_at
    from joined_data
    group by 1
)

select *
from final_cte

on conflict on constraint user_devices_cumulated_pkey do update
set
    device_activity_datelist = excluded.device_activity_datelist,
    last_updated_at = excluded.last_updated_at;
