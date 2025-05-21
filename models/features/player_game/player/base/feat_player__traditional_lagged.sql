{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'lagged', 'traditional'],
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
    -- Look back a fixed number of days to ensure calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest lag window.
    and game_date >= (
        select max(game_date) - interval '90 days' -- Adjust interval if longer lags are needed
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

        -- Lagged Traditional Boxscore Stats
        {{ calculate_lag('min', 'player_id', 'game_date', 1) }} as min_lag_1g,
        {{ calculate_lag('min', 'player_id', 'game_date', 3) }} as min_lag_3g,
        {{ calculate_lag('min', 'player_id', 'game_date', 5) }} as min_lag_5g,
        {{ calculate_lag('min', 'player_id', 'game_date', 7) }} as min_lag_7g,
        
        {{ calculate_lag('pts', 'player_id', 'game_date', 1) }} as pts_lag_1g,
        {{ calculate_lag('pts', 'player_id', 'game_date', 3) }} as pts_lag_3g,
        {{ calculate_lag('pts', 'player_id', 'game_date', 5) }} as pts_lag_5g,
        {{ calculate_lag('pts', 'player_id', 'game_date', 7) }} as pts_lag_7g,

        {{ calculate_lag('fgm', 'player_id', 'game_date', 1) }} as fgm_lag_1g,
        {{ calculate_lag('fgm', 'player_id', 'game_date', 3) }} as fgm_lag_3g,
        {{ calculate_lag('fgm', 'player_id', 'game_date', 5) }} as fgm_lag_5g,
        {{ calculate_lag('fgm', 'player_id', 'game_date', 7) }} as fgm_lag_7g,

        {{ calculate_lag('fga', 'player_id', 'game_date', 1) }} as fga_lag_1g,
        {{ calculate_lag('fga', 'player_id', 'game_date', 3) }} as fga_lag_3g,
        {{ calculate_lag('fga', 'player_id', 'game_date', 5) }} as fga_lag_5g,
        {{ calculate_lag('fga', 'player_id', 'game_date', 7) }} as fga_lag_7g,

        {{ calculate_lag('fg_pct', 'player_id', 'game_date', 1) }} as fg_pct_lag_1g,
        {{ calculate_lag('fg_pct', 'player_id', 'game_date', 3) }} as fg_pct_lag_3g,
        {{ calculate_lag('fg_pct', 'player_id', 'game_date', 5) }} as fg_pct_lag_5g,
        {{ calculate_lag('fg_pct', 'player_id', 'game_date', 7) }} as fg_pct_lag_7g,

        {{ calculate_lag('reb', 'player_id', 'game_date', 1) }} as reb_lag_1g,
        {{ calculate_lag('reb', 'player_id', 'game_date', 3) }} as reb_lag_3g,
        {{ calculate_lag('reb', 'player_id', 'game_date', 5) }} as reb_lag_5g,
        {{ calculate_lag('reb', 'player_id', 'game_date', 7) }} as reb_lag_7g,

        {{ calculate_lag('off_reb', 'player_id', 'game_date', 1) }} as off_reb_lag_1g,
        {{ calculate_lag('off_reb', 'player_id', 'game_date', 3) }} as off_reb_lag_3g,
        {{ calculate_lag('off_reb', 'player_id', 'game_date', 5) }} as off_reb_lag_5g,
        {{ calculate_lag('off_reb', 'player_id', 'game_date', 7) }} as off_reb_lag_7g,

        {{ calculate_lag('def_reb', 'player_id', 'game_date', 1) }} as def_reb_lag_1g,
        {{ calculate_lag('def_reb', 'player_id', 'game_date', 3) }} as def_reb_lag_3g,
        {{ calculate_lag('def_reb', 'player_id', 'game_date', 5) }} as def_reb_lag_5g,
        {{ calculate_lag('def_reb', 'player_id', 'game_date', 7) }} as def_reb_lag_7g,

        {{ calculate_lag('ast', 'player_id', 'game_date', 1) }} as ast_lag_1g,
        {{ calculate_lag('ast', 'player_id', 'game_date', 3) }} as ast_lag_3g,
        {{ calculate_lag('ast', 'player_id', 'game_date', 5) }} as ast_lag_5g,
        {{ calculate_lag('ast', 'player_id', 'game_date', 7) }} as ast_lag_7g,

        {{ calculate_lag('fg3m', 'player_id', 'game_date', 1) }} as fg3m_lag_1g,
        {{ calculate_lag('fg3m', 'player_id', 'game_date', 3) }} as fg3m_lag_3g,
        {{ calculate_lag('fg3m', 'player_id', 'game_date', 5) }} as fg3m_lag_5g,
        {{ calculate_lag('fg3m', 'player_id', 'game_date', 7) }} as fg3m_lag_7g,

        {{ calculate_lag('fg3a', 'player_id', 'game_date', 1) }} as fg3a_lag_1g,
        {{ calculate_lag('fg3a', 'player_id', 'game_date', 3) }} as fg3a_lag_3g,
        {{ calculate_lag('fg3a', 'player_id', 'game_date', 5) }} as fg3a_lag_5g,
        {{ calculate_lag('fg3a', 'player_id', 'game_date', 7) }} as fg3a_lag_7g,

        {{ calculate_lag('fg3_pct', 'player_id', 'game_date', 1) }} as fg3_pct_lag_1g,
        {{ calculate_lag('fg3_pct', 'player_id', 'game_date', 3) }} as fg3_pct_lag_3g,
        {{ calculate_lag('fg3_pct', 'player_id', 'game_date', 5) }} as fg3_pct_lag_5g,
        {{ calculate_lag('fg3_pct', 'player_id', 'game_date', 7) }} as fg3_pct_lag_7g,

        {{ calculate_lag('ftm', 'player_id', 'game_date', 1) }} as ftm_lag_1g,
        {{ calculate_lag('ftm', 'player_id', 'game_date', 3) }} as ftm_lag_3g,
        {{ calculate_lag('ftm', 'player_id', 'game_date', 5) }} as ftm_lag_5g,
        {{ calculate_lag('ftm', 'player_id', 'game_date', 7) }} as ftm_lag_7g,

        {{ calculate_lag('fta', 'player_id', 'game_date', 1) }} as fta_lag_1g,
        {{ calculate_lag('fta', 'player_id', 'game_date', 3) }} as fta_lag_3g,
        {{ calculate_lag('fta', 'player_id', 'game_date', 5) }} as fta_lag_5g,
        {{ calculate_lag('fta', 'player_id', 'game_date', 7) }} as fta_lag_7g,

        {{ calculate_lag('ft_pct', 'player_id', 'game_date', 1) }} as ft_pct_lag_1g,
        {{ calculate_lag('ft_pct', 'player_id', 'game_date', 3) }} as ft_pct_lag_3g,
        {{ calculate_lag('ft_pct', 'player_id', 'game_date', 5) }} as ft_pct_lag_5g,
        {{ calculate_lag('ft_pct', 'player_id', 'game_date', 7) }} as ft_pct_lag_7g,

        {{ calculate_lag('stl', 'player_id', 'game_date', 1) }} as stl_lag_1g,
        {{ calculate_lag('stl', 'player_id', 'game_date', 3) }} as stl_lag_3g,
        {{ calculate_lag('stl', 'player_id', 'game_date', 5) }} as stl_lag_5g,
        {{ calculate_lag('stl', 'player_id', 'game_date', 7) }} as stl_lag_7g,

        {{ calculate_lag('blk', 'player_id', 'game_date', 1) }} as blk_lag_1g,
        {{ calculate_lag('blk', 'player_id', 'game_date', 3) }} as blk_lag_3g,
        {{ calculate_lag('blk', 'player_id', 'game_date', 5) }} as blk_lag_5g,
        {{ calculate_lag('blk', 'player_id', 'game_date', 7) }} as blk_lag_7g,

        {{ calculate_lag('tov', 'player_id', 'game_date', 1) }} as tov_lag_1g,
        {{ calculate_lag('tov', 'player_id', 'game_date', 3) }} as tov_lag_3g,
        {{ calculate_lag('tov', 'player_id', 'game_date', 5) }} as tov_lag_5g,
        {{ calculate_lag('tov', 'player_id', 'game_date', 7) }} as tov_lag_7g,

        {{ calculate_lag('pf', 'player_id', 'game_date', 1) }} as pf_lag_1g,
        {{ calculate_lag('pf', 'player_id', 'game_date', 3) }} as pf_lag_3g,
        {{ calculate_lag('pf', 'player_id', 'game_date', 5) }} as pf_lag_5g,
        {{ calculate_lag('pf', 'player_id', 'game_date', 7) }} as pf_lag_7g,

        {{ calculate_lag('plus_minus', 'player_id', 'game_date', 1) }} as plus_minus_lag_1g,
        {{ calculate_lag('plus_minus', 'player_id', 'game_date', 3) }} as plus_minus_lag_3g,
        {{ calculate_lag('plus_minus', 'player_id', 'game_date', 5) }} as plus_minus_lag_5g,
        {{ calculate_lag('plus_minus', 'player_id', 'game_date', 7) }} as plus_minus_lag_7g,

        traditional_boxscore.updated_at

    from traditional_boxscore
)

select *
from final
