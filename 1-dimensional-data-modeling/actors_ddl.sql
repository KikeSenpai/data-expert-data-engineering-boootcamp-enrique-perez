create type film_stats as (
    film text,
    votes int,
    rating decimal,
    film_id text,
    film_year int
);


create table if not exists actors (
    actor text,
    actor_id text,
    filmography film_stats[],
    quality_class text,
    is_active boolean,
    primary key (actor_id)
);
