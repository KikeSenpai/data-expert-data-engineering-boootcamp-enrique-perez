with game_rank as (
    select
        *,
        row_number() over (
            partition by
                game_id,
                team_id,
                player_id
        ) as rnk
    from game_details
)

select *
from game_rank
where rnk = 1;
