{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'traditional'],
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

with player_boxscore_data as (
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

        -- Raw stats needed for new ratio calculations
        min,
        pf,
        fgm,
        fga,
        fg3m,
        fg3a,
        ftm,
        fta,
        possessions, -- from advanced stats

        -- Pre-calculated ratios from int_player__combined_boxscore
        pts_per_min,
        reb_per_min,
        ast_per_min,
        stl_per_min,
        blk_per_min,
        tov_per_min,
        pra_per_min,
        
        pts_per_36,
        reb_per_36,
        ast_per_36,
        stl_per_36,
        blk_per_36,
        tov_per_36,
        pra_per_36,
        
        pts_per_100,
        reb_per_100,
        ast_per_100,
        stl_per_100,
        blk_per_100,
        tov_per_100,
        pra_per_100,

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

        -- Pre-calculated ratios
        pts_per_min,
        reb_per_min,
        ast_per_min,
        stl_per_min,
        blk_per_min,
        tov_per_min,
        pra_per_min,
        
        pts_per_36,
        reb_per_36,
        ast_per_36,
        stl_per_36,
        blk_per_36,
        tov_per_36,
        pra_per_36,
        
        pts_per_100,
        reb_per_100,
        ast_per_100,
        stl_per_100,
        blk_per_100,
        tov_per_100,
        pra_per_100,

        -- Newly calculated Per-Minute Stats
        case when min > 0 then round((pf / min)::numeric, 3) else 0 end as pf_per_min,
        case when min > 0 then round((fgm / min)::numeric, 3) else 0 end as fgm_per_min,
        case when min > 0 then round((fga / min)::numeric, 3) else 0 end as fga_per_min,
        case when min > 0 then round((fg3m / min)::numeric, 3) else 0 end as fg3m_per_min,
        case when min > 0 then round((fg3a / min)::numeric, 3) else 0 end as fg3a_per_min,
        case when min > 0 then round((ftm / min)::numeric, 3) else 0 end as ftm_per_min,
        case when min > 0 then round((fta / min)::numeric, 3) else 0 end as fta_per_min,

        -- Newly calculated Per-36 Stats
        case when min > 0 then round(((pf / min) * 36)::numeric, 3) else 0 end as pf_per_36,
        case when min > 0 then round(((fgm / min) * 36)::numeric, 3) else 0 end as fgm_per_36,
        case when min > 0 then round(((fga / min) * 36)::numeric, 3) else 0 end as fga_per_36,
        case when min > 0 then round(((fg3m / min) * 36)::numeric, 3) else 0 end as fg3m_per_36,
        case when min > 0 then round(((fg3a / min) * 36)::numeric, 3) else 0 end as fg3a_per_36,
        case when min > 0 then round(((ftm / min) * 36)::numeric, 3) else 0 end as ftm_per_36,
        case when min > 0 then round(((fta / min) * 36)::numeric, 3) else 0 end as fta_per_36,

        -- Newly calculated Per-100 Possessions Stats
        case when possessions > 0 and min > 0 then round(((pf / possessions) * 100)::numeric, 3) else 0 end as pf_per_100,
        case when possessions > 0 and min > 0 then round(((fgm / possessions) * 100)::numeric, 3) else 0 end as fgm_per_100,
        case when possessions > 0 and min > 0 then round(((fga / possessions) * 100)::numeric, 3) else 0 end as fga_per_100,
        case when possessions > 0 and min > 0 then round(((fg3m / possessions) * 100)::numeric, 3) else 0 end as fg3m_per_100,
        case when possessions > 0 and min > 0 then round(((fg3a / possessions) * 100)::numeric, 3) else 0 end as fg3a_per_100,
        case when possessions > 0 and min > 0 then round(((ftm / possessions) * 100)::numeric, 3) else 0 end as ftm_per_100,
        case when possessions > 0 and min > 0 then round(((fta / possessions) * 100)::numeric, 3) else 0 end as fta_per_100,
        
        updated_at
    from player_boxscore_data
)

select *
from final
