-- What is the most games a team has won in a 90 game stretch?
with game_details_enriched as (
    select
        gd.game_id,
        gd.team_abbreviation as team,
        g.game_date_est as game_date,
        bool_and(case
            when g.home_team_wins = 1 and gd.team_id = g.home_team_id then true
            else false
        end) as team_won
    from game_details as gd
    left join games as g
        on gd.game_id = g.game_id
    group by 1, 2, 3
),

window_analysis as (
    select
        *,
        count(game_id) over (
            partition by team
            order by game_date
            rows between 89 preceding and current row
        ) as cumulative_games,
        sum(case when team_won then 1 else 0 end) over (
            partition by team
            order by game_date
            rows between 89 preceding and current row
        ) as num_games_won_last_90_games
    from game_details_enriched
)

select
    team,
    max(num_games_won_last_90_games) as most_games_won_90_games
from window_analysis
where cumulative_games = 90
group by 1
order by 2 desc;


-- How many games in a row did LeBron James score over 10 points a game?
with game_details_enriched as (
    select
        gd.game_id,
        g.game_date_est as game_date,
        case 
            when coalesce(gd.pts, 0) > 10 then 1
            else 0
        end as is_good_score
    from game_details as gd
    left join games as g
        on gd.game_id = g.game_id
    where gd.player_name = 'LeBron James'
),

scoring_streak as (
    select
        *,
        case
            when is_good_score != lag(is_good_score) over (order by game_date) then 1
            else 0
        end as is_new_streak
    from game_details_enriched
),

winning_streak_id as (
    select
        *,
        sum(is_new_streak) over (order by game_date) + 1 as streak_id
    from scoring_streak
)

select
    streak_id,
    sum(is_good_score)
from winning_streak_id
group by 1
order by 2 desc;
