insert into actors_history_scd (actor, actor_id, quality_class, start_date, end_date, is_active)
with all_years as (
    select 
        generate_series(
            (select min(year) from actor_films),
            (select max(year) from actor_films),
            1
        ) as year
),

yearly_rating as (
    select
        actor,
        actorid as actor_id,
        year,
        avg(rating) as year_rating
    from actor_films
    group by 1, 2, 3
),

all_actors_all_years as (
    select *
    from all_years
    cross join (
        select distinct
            actor,
            actor_id
        from yearly_rating
    )
),

is_active as (
    select
        ay.actor,
        ay.actor_id,
        ay.year,
        coalesce(yr.year_rating, 0) as year_rating,
        case
            when yr.year_rating is null then false
            when yr.year_rating > 0 then true
        end as is_active
    from all_actors_all_years as ay
    left join yearly_rating as yr
        on ay.actor_id = yr.actor_id
        and ay.actor = yr.actor
        and ay.year = yr.year
),

quality_class as (
    select
        actor,
        actor_id,
        year,
        is_active,
        case
            when year_rating > 8 then 'star'
            when year_rating > 7 and year_rating <= 8 then 'good'
            when year_rating > 6 and year_rating <= 7 then 'average'
            when year_rating <= 6 then 'bad'
        end as quality_class
    from is_active
),

previous_year_quality_class as (
    select
        *,
        lag(quality_class) over (
            partition by actor, actor_id
            order by year
        ) as previous_year_quality_class,
        lag(is_active) over (
            partition by actor, actor_id
            order by year
        ) as previous_year_active
    from quality_class
),

dimension_change as (
    select
        actor,
        actor_id,
        year,
        quality_class,
        is_active,
        case
            when
                quality_class != previous_year_quality_class
                or previous_year_quality_class is null
                then 1
            when
                is_active != previous_year_active
                or previous_year_active is null
                then 1
            else 0
        end as has_dimension_changed
    from previous_year_quality_class
),

dimension_change_id as (
    select
        actor,
        actor_id,
        year,
        quality_class,
        is_active,
        sum(has_dimension_changed) over (
            partition by actor, actor_id
            order by year
        ) as dimension_changed_id
    from dimension_change
),

start_end_date as (
    select
        actor,
        actor_id,
        dimension_changed_id,
        (array_agg(quality_class order by year desc))[1] as quality_class,
        (array_agg(is_active order by year desc))[1] as is_active,
        case
            when 
                dimension_changed_id = 1 
                then max(make_date(year, 1, 1))
            else min(make_date(year, 1, 1))
        end as start_date,
        max(make_date(year, 12, 31)) as end_date
    from dimension_change_id
    group by 1, 2, 3
)

select *
from start_end_date;
