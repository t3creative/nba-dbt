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
        player_name,
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

lagged_stats as (
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

        -- Traditional Boxscore Stats (current game)
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
),

final as (
    select
        *, -- Select all columns from lagged_stats

        -- Lagged Delta Calculations (Current Stat - Lagged Stat)
        min - min_lag_1g as min_delta_lag_1g,
        min - min_lag_3g as min_delta_lag_3g,
        min - min_lag_5g as min_delta_lag_5g,
        min - min_lag_7g as min_delta_lag_7g,

        pts - pts_lag_1g as pts_delta_lag_1g,
        pts - pts_lag_3g as pts_delta_lag_3g,
        pts - pts_lag_5g as pts_delta_lag_5g,
        pts - pts_lag_7g as pts_delta_lag_7g,

        fgm - fgm_lag_1g as fgm_delta_lag_1g,
        fgm - fgm_lag_3g as fgm_delta_lag_3g,
        fgm - fgm_lag_5g as fgm_delta_lag_5g,
        fgm - fgm_lag_7g as fgm_delta_lag_7g,

        fga - fga_lag_1g as fga_delta_lag_1g,
        fga - fga_lag_3g as fga_delta_lag_3g,
        fga - fga_lag_5g as fga_delta_lag_5g,
        fga - fga_lag_7g as fga_delta_lag_7g,

        fg_pct - fg_pct_lag_1g as fg_pct_delta_lag_1g,
        fg_pct - fg_pct_lag_3g as fg_pct_delta_lag_3g,
        fg_pct - fg_pct_lag_5g as fg_pct_delta_lag_5g,
        fg_pct - fg_pct_lag_7g as fg_pct_delta_lag_7g,

        reb - reb_lag_1g as reb_delta_lag_1g,
        reb - reb_lag_3g as reb_delta_lag_3g,
        reb - reb_lag_5g as reb_delta_lag_5g,
        reb - reb_lag_7g as reb_delta_lag_7g,

        off_reb - off_reb_lag_1g as off_reb_delta_lag_1g,
        off_reb - off_reb_lag_3g as off_reb_delta_lag_3g,
        off_reb - off_reb_lag_5g as off_reb_delta_lag_5g,
        off_reb - off_reb_lag_7g as off_reb_delta_lag_7g,

        def_reb - def_reb_lag_1g as def_reb_delta_lag_1g,
        def_reb - def_reb_lag_3g as def_reb_delta_lag_3g,
        def_reb - def_reb_lag_5g as def_reb_delta_lag_5g,
        def_reb - def_reb_lag_7g as def_reb_delta_lag_7g,

        ast - ast_lag_1g as ast_delta_lag_1g,
        ast - ast_lag_3g as ast_delta_lag_3g,
        ast - ast_lag_5g as ast_delta_lag_5g,
        ast - ast_lag_7g as ast_delta_lag_7g,

        fg3m - fg3m_lag_1g as fg3m_delta_lag_1g,
        fg3m - fg3m_lag_3g as fg3m_delta_lag_3g,
        fg3m - fg3m_lag_5g as fg3m_delta_lag_5g,
        fg3m - fg3m_lag_7g as fg3m_delta_lag_7g,

        fg3a - fg3a_lag_1g as fg3a_delta_lag_1g,
        fg3a - fg3a_lag_3g as fg3a_delta_lag_3g,
        fg3a - fg3a_lag_5g as fg3a_delta_lag_5g,
        fg3a - fg3a_lag_7g as fg3a_delta_lag_7g,

        fg3_pct - fg3_pct_lag_1g as fg3_pct_delta_lag_1g,
        fg3_pct - fg3_pct_lag_3g as fg3_pct_delta_lag_3g,
        fg3_pct - fg3_pct_lag_5g as fg3_pct_delta_lag_5g,
        fg3_pct - fg3_pct_lag_7g as fg3_pct_delta_lag_7g,

        ftm - ftm_lag_1g as ftm_delta_lag_1g,
        ftm - ftm_lag_3g as ftm_delta_lag_3g,
        ftm - ftm_lag_5g as ftm_delta_lag_5g,
        ftm - ftm_lag_7g as ftm_delta_lag_7g,

        fta - fta_lag_1g as fta_delta_lag_1g,
        fta - fta_lag_3g as fta_delta_lag_3g,
        fta - fta_lag_5g as fta_delta_lag_5g,
        fta - fta_lag_7g as fta_delta_lag_7g,

        ft_pct - ft_pct_lag_1g as ft_pct_delta_lag_1g,
        ft_pct - ft_pct_lag_3g as ft_pct_delta_lag_3g,
        ft_pct - ft_pct_lag_5g as ft_pct_delta_lag_5g,
        ft_pct - ft_pct_lag_7g as ft_pct_delta_lag_7g,

        stl - stl_lag_1g as stl_delta_lag_1g,
        stl - stl_lag_3g as stl_delta_lag_3g,
        stl - stl_lag_5g as stl_delta_lag_5g,
        stl - stl_lag_7g as stl_delta_lag_7g,

        blk - blk_lag_1g as blk_delta_lag_1g,
        blk - blk_lag_3g as blk_delta_lag_3g,
        blk - blk_lag_5g as blk_delta_lag_5g,
        blk - blk_lag_7g as blk_delta_lag_7g,

        tov - tov_lag_1g as tov_delta_lag_1g,
        tov - tov_lag_3g as tov_delta_lag_3g,
        tov - tov_lag_5g as tov_delta_lag_5g,
        tov - tov_lag_7g as tov_delta_lag_7g,

        pf - pf_lag_1g as pf_delta_lag_1g,
        pf - pf_lag_3g as pf_delta_lag_3g,
        pf - pf_lag_5g as pf_delta_lag_5g,
        pf - pf_lag_7g as pf_delta_lag_7g,

        plus_minus - plus_minus_lag_1g as plus_minus_delta_lag_1g,
        plus_minus - plus_minus_lag_3g as plus_minus_delta_lag_3g,
        plus_minus - plus_minus_lag_5g as plus_minus_delta_lag_5g,
        plus_minus - plus_minus_lag_7g as plus_minus_delta_lag_7g

    from lagged_stats
)

select *
from final
