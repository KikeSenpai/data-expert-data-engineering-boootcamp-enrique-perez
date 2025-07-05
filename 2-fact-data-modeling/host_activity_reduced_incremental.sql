insert into host_activity_reduced
with yesterday as (
    select *
    from host_activity_reduced
    where date_partition = '2023-01-01'
),

today as (
    select
        host,
        cast(event_time as date) AS today_date,
        count(1) as num_of_hits,
        count(distinct user_id) as num_of_unique_visitors
    from events
    where
        cast(event_time as date) = '2023-01-02'
        and user_id is not null
    group by 1, 2
),

final_cte as (
    select
        coalesce(y.host, t.host) as host,
        coalesce(
            y.hit_array,
            array_fill(null::bigint, array['2023-01-02'::date - '2023-01-01'::date])
        ) || array[t.num_of_hits] as hits_array,
        coalesce(
            y.unique_visitors,
            array_fill(null::bigint, array['2023-01-02'::date - '2023-01-01'::date])
        ) || array[t.num_of_unique_visitors] as unique_visitors,
        '2023-01-01'::date as "month",
        '2023-01-02'::date AS date_partition
    from yesterday as y
    full outer join today as t
        ON y.host = t.host
)

select * from final_cte;
