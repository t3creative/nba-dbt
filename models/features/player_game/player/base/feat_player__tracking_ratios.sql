{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'tracking'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with player_tracking_base_data as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        player_name,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Base counts for normalization
        min,
        possessions,

        -- Player Tracking Stats from int_player__combined_boxscore
        speed,
        distance,
        off_reb_chances,
        def_reb_chances,
        reb_chances,
        touches,
        secondary_ast,
        ft_ast,
        passes,
        cont_fgm, -- Will use FGA for rate of attempts, FGM could be useful too
        cont_fga,
        cont_fg_pct,
        uncont_fgm, -- Will use FGA for rate of attempts
        uncont_fga,
        uncont_fg_pct,
        def_at_rim_fgm, -- Will use FGA for rate of attempts
        def_at_rim_fga,
        def_at_rim_fg_pct,

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

final as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        player_name,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Existing Tracking Percentages
        cont_fg_pct,
        uncont_fg_pct,
        def_at_rim_fg_pct,
        speed, -- Speed is a rate itself, not typically further normalized by min/poss

        -- Per Minute Ratios
        case when min > 0 then round((distance / min)::numeric, 3) else 0 end as distance_per_min,
        case when min > 0 then round((touches / min)::numeric, 3) else 0 end as touches_per_min,
        case when min > 0 then round((passes / min)::numeric, 3) else 0 end as passes_per_min,
        case when min > 0 then round((secondary_ast / min)::numeric, 3) else 0 end as secondary_ast_per_min,
        case when min > 0 then round((ft_ast / min)::numeric, 3) else 0 end as ft_ast_per_min,
        case when min > 0 then round((off_reb_chances / min)::numeric, 3) else 0 end as off_reb_chances_per_min,
        case when min > 0 then round((def_reb_chances / min)::numeric, 3) else 0 end as def_reb_chances_per_min,
        case when min > 0 then round((reb_chances / min)::numeric, 3) else 0 end as reb_chances_per_min,
        case when min > 0 then round((cont_fga / min)::numeric, 3) else 0 end as cont_fga_per_min,
        case when min > 0 then round((uncont_fga / min)::numeric, 3) else 0 end as uncont_fga_per_min,
        case when min > 0 then round((def_at_rim_fga / min)::numeric, 3) else 0 end as def_at_rim_fga_per_min, -- Rate of defending shots at rim

        -- Per 100 Possessions Ratios
        case when possessions > 0 and min > 0 then round(((distance / possessions) * 100)::numeric, 3) else 0 end as distance_per_100_poss, -- Distance per 100 offensive possessions player is on for
        case when possessions > 0 and min > 0 then round(((touches / possessions) * 100)::numeric, 3) else 0 end as touches_per_100_poss,
        case when possessions > 0 and min > 0 then round(((passes / possessions) * 100)::numeric, 3) else 0 end as passes_per_100_poss,
        case when possessions > 0 and min > 0 then round(((secondary_ast / possessions) * 100)::numeric, 3) else 0 end as secondary_ast_per_100_poss,
        case when possessions > 0 and min > 0 then round(((ft_ast / possessions) * 100)::numeric, 3) else 0 end as ft_ast_per_100_poss,
        case when possessions > 0 and min > 0 then round(((off_reb_chances / possessions) * 100)::numeric, 3) else 0 end as off_reb_chances_per_100_poss,
        case when possessions > 0 and min > 0 then round(((def_reb_chances / possessions) * 100)::numeric, 3) else 0 end as def_reb_chances_per_100_poss, -- Might be less direct if possessions is purely offensive
        case when possessions > 0 and min > 0 then round(((reb_chances / possessions) * 100)::numeric, 3) else 0 end as reb_chances_per_100_poss,
        case when possessions > 0 and min > 0 then round(((cont_fga / possessions) * 100)::numeric, 3) else 0 end as cont_fga_per_100_poss,
        case when possessions > 0 and min > 0 then round(((uncont_fga / possessions) * 100)::numeric, 3) else 0 end as uncont_fga_per_100_poss,
        case when possessions > 0 and min > 0 then round(((def_at_rim_fga / possessions) * 100)::numeric, 3) else 0 end as def_at_rim_fga_per_100_poss, -- Rate of defending shots at rim per 100 offensive possessions (could be a proxy for overall game flow)
        
        updated_at
    from player_tracking_base_data
)

select *
from final
