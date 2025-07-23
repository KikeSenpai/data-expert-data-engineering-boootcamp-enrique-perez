with game_details_enriched as (
    select
        gd.game_id,
        gd.player_name,
        gd.team_abbreviation,
        gd.pts,
        g.season::text as season,
        case
            when g.home_team_wins = 1 and gd.team_id = g.home_team_id then 1
            else 0
        end as team_id_won
    from game_details as gd
    left join games as g
        on gd.game_id = g.game_id
),

grouping_aggregations as (
    select
        case
            when grouping(player_name) = 0 and grouping(season) = 0 then 'PLAYER_SEASON'
            when grouping(player_name) = 0 and grouping(team_abbreviation) = 0 then 'PLAYER_TEAM'
            when grouping(team_abbreviation) = 0 then 'TEAM'
        end as aggregation_level,
        coalesce(player_name, 'OVERALL') as player_name,
        coalesce(team_abbreviation, 'OVERALL') as team,
        coalesce(season, 'OVERALL') as season,
        coalesce(sum(pts), 0) as total_points,
        count(distinct case when team_id_won = 1 then game_id end) as num_of_won_games
    from game_details_enriched
    group by 
        grouping sets(
            (player_name, team_abbreviation),
            (player_name, season),
            (team_abbreviation)
        )
)

-- Player who scored the most points for a single team.
player_team_most_points as (
    select
        player_name,
        team,
        total_points
    from grouping_aggregations
    where aggregation_level = 'PLAYER_TEAM'
    order by total_points desc
    limit 1
),

-- Player who scored the most points in a single season.
player_season_most_points as (
    select
        player_name,
        season,
        total_points
    from grouping_aggregations
    where aggregation_level = 'PLAYER_SEASON'
    order by total_points desc
    limit 1
),

-- Team with the most total wins.
team_most_wins as (
    select
        team,
        num_of_won_games
    from grouping_aggregations
    where aggregation_level = 'TEAM'
    order by num_of_won_games desc
    limit 1
)

-- Query the appropiate CTE to answer each question
select * from team_most_wins;


