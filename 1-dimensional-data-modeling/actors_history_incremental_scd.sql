with new_year_actor_films as (
    select
        actor,
        actorid as actor_id,
        year,
        avg(rating) as year_rating
    from actor_films
    where year = (
        select max(date_part('year', end_date))
        from actors_history_incremental_scd
    ) + 1
    group by 1, 2, 3
),

current_year as (
    select
        actor,
        actor_id,
        quality_class,
        is_active,
        start_date,
        end_date
    from actors_history_incremental_scd
    where is_active is true
),

new_year as (
    select
        actor,
        actor_id,
        make_date(year, 1, 1) as start_date,
        make_date(year, 12, 31) as end_date,
        case when year_rating > 0 then true else false end as is_active,
        case
            when year_rating > 8 then 'star'
            when year_rating > 7 and year_rating <= 8 then 'good'
            when year_rating > 6 and year_rating <= 7 then 'average'
            when year_rating <= 6 then 'bad'
        end as quality_class
    from new_year_actor_films
),

unchanged_records as (
    select
        ny.actor,
        ny.actor_id,
        ny.quality_class,
        ny.is_active,
        cy.start_date,
        ny.end_date
    from new_year as ny
    inner join current_year as cy
        on cy.actor_id = ny.actor_id
        and cy.quality_class = ny.quality_class
        and cy.is_active = ny.is_active
),

changed_records as (
    select
        ny.actor,
        ny.actor_id,
        ny.quality_class,
        ny.is_active,
        ny.start_date,
        ny.end_date
    from new_year as ny
    left join current_year as cy
        on cy.actor_id = ny.actor_id
    where
        cy.quality_class != ny.quality_class
        or cy.is_active != ny.is_active
),

not_active_records as (
    select
        cy.actor,
        cy.actor_id,
        cy.quality_class,
        false as is_active,
        cy.start_date,
        cy.end_date
    from current_year as cy
    left join new_year as ny
        on cy.actor_id = ny.actor_id
    where ny.actor_id is null
),

new_records as (
    select
        ny.actor,
        ny.actor_id,
        ny.quality_class,
        ny.is_active,
        ny.start_date,
        ny.end_date
    from new_year as ny
    left join current_year as cy
        on cy.actor_id = ny.actor_id
    where cy.actor_id is null
),

final_cte as (
    select *
    from unchanged_records
    union all
    select *
    from not_active_records
    union all
    select *
    from new_records
    union all
    select *
    from changed_records
)

select *
from final_cte;


