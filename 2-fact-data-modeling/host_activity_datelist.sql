-- Initial table population
insert into hosts_cumulated
with initial_events as (
    select
        host,
        cast(event_time as date) as event_date
    from events
    where cast(event_time as date) = '2023-01-01'
),

final_cte as (
    select
        host,
        array_agg(distinct event_date) as activity_datelist
    from initial_events
    group by 1
)

select
    *,
    '2023-01-01'::date as last_updated_at
from final_cte;


select * from hosts_cumulated;


-- Cumulative query
insert into hosts_cumulated
with yesterday_events as (
    select
        host,
        cast(event_time as date) as event_date
    from events
    where cast(event_time as date) = (select max(last_updated_at) from hosts_cumulated) + 1
),

yesterday_activity_datelist as (
    select
        host,
        array_agg(distinct event_date) as activity_datelist
    from yesterday_events
    group by 1
),

joined_data as (
    select
        coalesce(c.host, y.host) as host,
        case
            when 
                c.host is not null and y.host is not null
                then c.activity_datelist || y.activity_datelist
            when c.host is null then y.activity_datelist
            else c.activity_datelist
        end as activity_datelist,
        (select max(last_updated_at) from hosts_cumulated) + 1 as last_updated_at
    from hosts_cumulated as c
    full outer join yesterday_activity_datelist as y
        on c.host = y.host
)

select *
from joined_data

on conflict on constraint hosts_cumulated_pkey do update
set
    activity_datelist = excluded.activity_datelist,
    last_updated_at = excluded.last_updated_at;
