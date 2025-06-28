-- Initial table population year 1970
insert into actors
with film_statistics as (
    select
        actor,
        actorid as actor_id,
        (film, votes, rating, filmid, "year")::film_stats as film_statistics
    from actor_films
    where year = 1970
),

filmography as (
    select
        actor,
        actor_id,
        array_agg(film_statistics) as filmography,
        avg((film_statistics).rating) as avg_rating
    from film_statistics
    group by 1, 2
),

quality_class as (
    select
        actor,
        actor_id,
        filmography,
        case
            when avg_rating > 8 then 'star'
            when avg_rating > 7 and avg_rating <= 8 then 'good'
            when avg_rating > 6 and avg_rating <= 7 then 'average'
            when avg_rating <= 6 then 'bad'
        end as quality_class,
        true as is_active
    from filmography
)

select *
from quality_class;


-- Cumulative table design query for actors
insert into actors
with current_year_actor_films as (
    select
        actor,
        actorid as actor_id,
        (film, votes, rating, filmid, "year")::film_stats as film_statistics
    from actor_films
    where year = (
        select max((film_stats).film_year)
        from
            actors,
            lateral unnest(filmography) as film_stats
    ) + 1
),

current_year_filmography as (
    select
        actor,
        actor_id,
        array_agg(film_statistics) as filmography,
        avg((film_statistics).rating) as avg_rating
    from current_year_actor_films
    group by 1, 2
),

current_year_actors as (
    select
        actor,
        actor_id,
        filmography,
        case
            when avg_rating > 8 then 'star'
            when avg_rating > 7 and avg_rating <= 8 then 'good'
            when avg_rating > 6 and avg_rating <= 7 then 'average'
            when avg_rating <= 6 then 'bad'
        end as quality_class,
        true as is_active
    from current_year_filmography
),

joined_data as (
    select
        coalesce(py.actor, cy.actor) as actor,
        coalesce(py.actor_id, cy.actor_id) as actor_id,
        py.filmography || cy.filmography as filmography,
        coalesce(cy.quality_class, py.quality_class) as quality_class,
        coalesce(cy.is_active, false) as is_active
    from actors as py
    full outer join current_year_actors as cy
        on py.actor_id = cy.actor_id
        and py.actor = cy.actor
)

select *
from joined_data

on conflict on constraint actors_pkey do update
set
    filmography = excluded.filmography,
    quality_class = excluded.quality_class,
    is_active = excluded.is_active;
