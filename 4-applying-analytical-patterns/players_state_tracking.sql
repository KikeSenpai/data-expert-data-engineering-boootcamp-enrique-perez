create type season_stats as (
    season int,
    pts real,
    ast real,
    reb real,
    weight int
);


create type scoring_class as enum (
    'bad',
    'average',
    'good',
    'star'
);

create table if not exists players_state_tracking (
    player_name text,
    height text,
    college text,
    country text,
    draft_year text,
    draft_round text,
    draft_number text,
    seasons season_stats[],
    scoring_class scoring_class,
    years_since_last_active int,
    is_active boolean,
    player_state text,
    current_season int,
    primary key (player_name)
);


-- Initial population query for players
insert into players_state_tracking
with initial_players as (
    select
        player_name,
        height,
        college,
        country,
        draft_year,
        draft_round,
        draft_number,
        array[(season, pts, ast, reb, weight)::season_stats] as seasons,
        (case
            when pts > 20 then 'star'
            when pts > 15 then 'good'
            when pts > 10 then 'average'
            else 'bad'
        end)::scoring_class as scoring_class,
        0 as years_since_last_active,
        true as is_active,
        'New' as player_state,
        1996 as current_season
    from player_seasons
    where season = 1996
)

select *
from initial_players;


-- Incremental population query for players
insert into players_state_tracking
with previous_season as (
    select *
    from players_state_tracking
    where current_season = 1997
),

current_season as (
    select
        player_name,
        height,
        college,
        country,
        draft_year,
        draft_round,
        draft_number,
        array[(season, pts, ast, reb, weight)::season_stats] as seasons,
        (case
        when pts > 20 then 'star'
        when pts > 15 then 'good'
        when pts > 10 then 'average'
        else 'bad'
        end)::scoring_class as scoring_class,
        true as is_active
    from player_seasons
    where season = 1998
),

final_cte as (
    select
        coalesce(p.player_name, c.player_name) as player_name,
        coalesce(p.height, c.height) as height,
        coalesce(p.college, c.college) as college,
        coalesce(p.country, c.country) as country,
        coalesce(p.draft_year, c.draft_year) as draft_year,
        coalesce(p.draft_round, c.draft_round) as draft_round,
        coalesce(p.draft_number, c.draft_number) as draft_number,
        coalesce(p.seasons, array[]::season_stats[]) || c.seasons as seasons,
        coalesce(c.scoring_class, p.scoring_class) as scoring_class,
        case
        when c.is_active is null then p.years_since_last_active + 1 
        when c.is_active is true then 0
        end as years_since_last_active,
        coalesce(c.is_active, false) as is_active,
        case
        when p.is_active is null and c.is_active is true then 'New'
        when c.is_active is null then 'Retired'
        when p.is_active is true and c.is_active is true then 'Continued Playing'
        when p.is_active is false and c.is_active is true then 'Returned from Retirement'
        when p.is_active is false and c.is_active is null then 'Stayed Retired'
        else 'Unknown'
        end as player_state,
        1998 as current_season
    from previous_season as p
    full outer join current_season as c
        on p.player_name = c.player_name
)

select *
from final_cte

on conflict on constraint players_state_tracking_pkey do update
set
    seasons = excluded.seasons,
    scoring_class = excluded.scoring_class,
    years_since_last_active = excluded.years_since_last_active,
    is_active = excluded.is_active,
    player_state = excluded.player_state,
    current_season = excluded.current_season;
