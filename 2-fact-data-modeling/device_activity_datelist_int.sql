create type browser_datelist_int as (
    browser_type text,
    datelist_int bit(32)
);


create table if not exists user_devices_datelist_int (
    user_id numeric not null,
    device_activity_datelist_int browser_datelist_int[],
    last_updated_at date,
    primary key (user_id)
);


insert into user_devices_datelist_int
with exploded_browser as (
    select
        user_id,
        (browser_datelist).browser_type,
        (browser_datelist).datelist
    from
        user_devices_cumulated,
        unnest(device_activity_datelist) as browser_datelist
),

exploded_dates as (
    select
        e.user_id,
        e.browser_type,
        case
            when d.elem is not null then true
            else false
        end as is_active,
        31 - d.pos as days_since
    from
        exploded_browser as e,
        unnest(datelist) with ordinality as d(elem, pos)
),

datelist_int as (
    select
        user_id,
        browser_type,
        cast(sum(case
            when is_active then pow(2, 32 - days_since)
            else 0
        end) as bigint)::bit(32) as datelist_int,
        '2023-01-31'::date as last_updated_at
    from exploded_dates
    group by 1, 2
),

final_cte as (
    select
        user_id,
        array_agg((browser_type, datelist_int)::browser_datelist_int) as device_activity_datelist_int,
        max(last_updated_at) as last_updated_at
    from datelist_int
    group by 1
)

select * from final_cte;
