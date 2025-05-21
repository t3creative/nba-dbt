{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'traditional'],
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

with traditional_boxscore as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Traditional Boxscore Stats
        min,
        pts,
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        reb,
        off_reb,
        def_reb,
        ast,
        stl,
        blk,
        tov,
        pf,
        plus_minus,

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= cast(substring('{{ var('training_start_season_year') }}' from 1 for 4) as integer)
    {% if is_incremental() %}
    -- Look back a fixed number of days to ensure rolling calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest rolling window.
    and game_date >= (
        select max(game_date) - interval '90 days'
        from {{ this }}
    )
    {% endif %}
),

final as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Traditional Boxscore Stats
        round(({{ calculate_rolling_avg('min', 'player_id', 'game_date', 3) }})::numeric, 3) as min_roll_3g_avg,
        round(({{ calculate_rolling_avg('min', 'player_id', 'game_date', 5) }})::numeric, 3) as min_roll_5g_avg,
        round(({{ calculate_rolling_avg('min', 'player_id', 'game_date', 10) }})::numeric, 3) as min_roll_10g_avg,
        round(({{ calculate_rolling_stddev('min', 'player_id', 'game_date', 3) }})::numeric, 3) as min_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('min', 'player_id', 'game_date', 5) }})::numeric, 3) as min_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('min', 'player_id', 'game_date', 10) }})::numeric, 3) as min_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('pts', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pts', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fga', 'player_id', 'game_date', 3) }})::numeric, 3) as fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('fga', 'player_id', 'game_date', 5) }})::numeric, 3) as fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('fga', 'player_id', 'game_date', 10) }})::numeric, 3) as fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fga', 'player_id', 'game_date', 3) }})::numeric, 3) as fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fga', 'player_id', 'game_date', 5) }})::numeric, 3) as fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fga', 'player_id', 'game_date', 10) }})::numeric, 3) as fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as fg_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('reb', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('reb', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('reb', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('reb', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('reb', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('reb', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ast', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('ast', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('ast', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ast', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ast', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ast', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3m_roll_3g_avg,
        round(({{ calculate_rolling_avg('fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3m_roll_5g_avg,
        round(({{ calculate_rolling_avg('fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3m_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3m_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3m_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3m_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3a_roll_3g_avg,
        round(({{ calculate_rolling_avg('fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3a_roll_5g_avg,
        round(({{ calculate_rolling_avg('fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3a_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3a_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3a_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3a_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fg3_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('fg3_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('fg3_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fg3_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as fg3_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fg3_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as fg3_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fg3_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as fg3_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ftm', 'player_id', 'game_date', 3) }})::numeric, 3) as ftm_roll_3g_avg,
        round(({{ calculate_rolling_avg('ftm', 'player_id', 'game_date', 5) }})::numeric, 3) as ftm_roll_5g_avg,
        round(({{ calculate_rolling_avg('ftm', 'player_id', 'game_date', 10) }})::numeric, 3) as ftm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ftm', 'player_id', 'game_date', 3) }})::numeric, 3) as ftm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ftm', 'player_id', 'game_date', 5) }})::numeric, 3) as ftm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ftm', 'player_id', 'game_date', 10) }})::numeric, 3) as ftm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fta', 'player_id', 'game_date', 3) }})::numeric, 3) as fta_roll_3g_avg,
        round(({{ calculate_rolling_avg('fta', 'player_id', 'game_date', 5) }})::numeric, 3) as fta_roll_5g_avg,
        round(({{ calculate_rolling_avg('fta', 'player_id', 'game_date', 10) }})::numeric, 3) as fta_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fta', 'player_id', 'game_date', 3) }})::numeric, 3) as fta_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fta', 'player_id', 'game_date', 5) }})::numeric, 3) as fta_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fta', 'player_id', 'game_date', 10) }})::numeric, 3) as fta_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ft_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ft_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('ft_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ft_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('ft_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ft_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ft_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as ft_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ft_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as ft_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ft_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as ft_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('stl', 'player_id', 'game_date', 3) }})::numeric, 3) as stl_roll_3g_avg,
        round(({{ calculate_rolling_avg('stl', 'player_id', 'game_date', 5) }})::numeric, 3) as stl_roll_5g_avg,
        round(({{ calculate_rolling_avg('stl', 'player_id', 'game_date', 10) }})::numeric, 3) as stl_roll_10g_avg,
        round(({{ calculate_rolling_stddev('stl', 'player_id', 'game_date', 3) }})::numeric, 3) as stl_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('stl', 'player_id', 'game_date', 5) }})::numeric, 3) as stl_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('stl', 'player_id', 'game_date', 10) }})::numeric, 3) as stl_roll_10g_stddev,

        round(({{ calculate_rolling_avg('blk', 'player_id', 'game_date', 3) }})::numeric, 3) as blk_roll_3g_avg,
        round(({{ calculate_rolling_avg('blk', 'player_id', 'game_date', 5) }})::numeric, 3) as blk_roll_5g_avg,
        round(({{ calculate_rolling_avg('blk', 'player_id', 'game_date', 10) }})::numeric, 3) as blk_roll_10g_avg,
        round(({{ calculate_rolling_stddev('blk', 'player_id', 'game_date', 3) }})::numeric, 3) as blk_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('blk', 'player_id', 'game_date', 5) }})::numeric, 3) as blk_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('blk', 'player_id', 'game_date', 10) }})::numeric, 3) as blk_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tov', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('tov', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('tov', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tov', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tov', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tov', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pf', 'player_id', 'game_date', 3) }})::numeric, 3) as pf_roll_3g_avg,
        round(({{ calculate_rolling_avg('pf', 'player_id', 'game_date', 5) }})::numeric, 3) as pf_roll_5g_avg,
        round(({{ calculate_rolling_avg('pf', 'player_id', 'game_date', 10) }})::numeric, 3) as pf_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pf', 'player_id', 'game_date', 3) }})::numeric, 3) as pf_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pf', 'player_id', 'game_date', 5) }})::numeric, 3) as pf_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pf', 'player_id', 'game_date', 10) }})::numeric, 3) as pf_roll_10g_stddev,

        round(({{ calculate_rolling_avg('plus_minus', 'player_id', 'game_date', 3) }})::numeric, 3) as plus_minus_roll_3g_avg,
        round(({{ calculate_rolling_avg('plus_minus', 'player_id', 'game_date', 5) }})::numeric, 3) as plus_minus_roll_5g_avg,
        round(({{ calculate_rolling_avg('plus_minus', 'player_id', 'game_date', 10) }})::numeric, 3) as plus_minus_roll_10g_avg,
        round(({{ calculate_rolling_stddev('plus_minus', 'player_id', 'game_date', 3) }})::numeric, 3) as plus_minus_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('plus_minus', 'player_id', 'game_date', 5) }})::numeric, 3) as plus_minus_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('plus_minus', 'player_id', 'game_date', 10) }})::numeric, 3) as plus_minus_roll_10g_stddev,

        traditional_boxscore.updated_at

    from traditional_boxscore
)

select *
from final 