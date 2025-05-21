{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'tracking'],
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

with tracking_boxscore as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Tracking Boxscore Stats
        distance,
        speed,
        touches,
        off_reb_chances,
        def_reb_chances,
        reb_chances,
        cont_fgm,
        cont_fga,
        cont_fg_pct,
        uncont_fgm,
        uncont_fga,
        uncont_fg_pct,
        def_at_rim_fgm,
        def_at_rim_fga,
        def_at_rim_fg_pct,
        secondary_ast, -- Added
        ft_ast,        -- Added
        passes,        -- Added

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

        -- Tracking Boxscore Stats
        round(({{ calculate_rolling_avg('distance', 'player_id', 'game_date', 3) }})::numeric, 3) as distance_roll_3g_avg,
        round(({{ calculate_rolling_avg('distance', 'player_id', 'game_date', 5) }})::numeric, 3) as distance_roll_5g_avg,
        round(({{ calculate_rolling_avg('distance', 'player_id', 'game_date', 10) }})::numeric, 3) as distance_roll_10g_avg,
        round(({{ calculate_rolling_stddev('distance', 'player_id', 'game_date', 3) }})::numeric, 3) as distance_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('distance', 'player_id', 'game_date', 5) }})::numeric, 3) as distance_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('distance', 'player_id', 'game_date', 10) }})::numeric, 3) as distance_roll_10g_stddev,

        round(({{ calculate_rolling_avg('speed', 'player_id', 'game_date', 3) }})::numeric, 3) as speed_roll_3g_avg,
        round(({{ calculate_rolling_avg('speed', 'player_id', 'game_date', 5) }})::numeric, 3) as speed_roll_5g_avg,
        round(({{ calculate_rolling_avg('speed', 'player_id', 'game_date', 10) }})::numeric, 3) as speed_roll_10g_avg,
        round(({{ calculate_rolling_stddev('speed', 'player_id', 'game_date', 3) }})::numeric, 3) as speed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('speed', 'player_id', 'game_date', 5) }})::numeric, 3) as speed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('speed', 'player_id', 'game_date', 10) }})::numeric, 3) as speed_roll_10g_stddev,

        round(({{ calculate_rolling_avg('touches', 'player_id', 'game_date', 3) }})::numeric, 3) as touches_roll_3g_avg,
        round(({{ calculate_rolling_avg('touches', 'player_id', 'game_date', 5) }})::numeric, 3) as touches_roll_5g_avg,
        round(({{ calculate_rolling_avg('touches', 'player_id', 'game_date', 10) }})::numeric, 3) as touches_roll_10g_avg,
        round(({{ calculate_rolling_stddev('touches', 'player_id', 'game_date', 3) }})::numeric, 3) as touches_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('touches', 'player_id', 'game_date', 5) }})::numeric, 3) as touches_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('touches', 'player_id', 'game_date', 10) }})::numeric, 3) as touches_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_chances_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_chances_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_chances_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as off_reb_chances_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as off_reb_chances_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as off_reb_chances_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_chances_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_chances_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_chances_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as def_reb_chances_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as def_reb_chances_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as def_reb_chances_roll_10g_stddev,

        round(({{ calculate_rolling_avg('reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_chances_roll_3g_avg,
        round(({{ calculate_rolling_avg('reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_chances_roll_5g_avg,
        round(({{ calculate_rolling_avg('reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_chances_roll_10g_avg,
        round(({{ calculate_rolling_stddev('reb_chances', 'player_id', 'game_date', 3) }})::numeric, 3) as reb_chances_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('reb_chances', 'player_id', 'game_date', 5) }})::numeric, 3) as reb_chances_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('reb_chances', 'player_id', 'game_date', 10) }})::numeric, 3) as reb_chances_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_fg_pct_roll_10g_stddev,
        
        round(({{ calculate_rolling_avg('uncont_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('uncont_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('uncont_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('uncont_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fgm_roll_10g_stddev,
        
        -- Added uncont_fga and uncont_fg_pct based on typical stats, assuming they exist in combined_boxscore
        round(({{ calculate_rolling_avg('uncont_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('uncont_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('uncont_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('uncont_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('uncont_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('uncont_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('uncont_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('uncont_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as uncont_fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as uncont_fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('uncont_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as uncont_fg_pct_roll_10g_stddev,
        
        -- Added def_at_rim_fgm and def_at_rim_fga based on typical stats, assuming they exist in combined_boxscore
        round(({{ calculate_rolling_avg('def_at_rim_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_at_rim_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_at_rim_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_at_rim_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_at_rim_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_at_rim_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_at_rim_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as def_at_rim_fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as def_at_rim_fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_at_rim_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as def_at_rim_fg_pct_roll_10g_stddev,

        -- Added rolling stats for secondary_ast
        round(({{ calculate_rolling_avg('secondary_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as secondary_ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('secondary_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as secondary_ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('secondary_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as secondary_ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('secondary_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as secondary_ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('secondary_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as secondary_ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('secondary_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as secondary_ast_roll_10g_stddev,

        -- Added rolling stats for ft_ast
        round(({{ calculate_rolling_avg('ft_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as ft_ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('ft_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as ft_ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('ft_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as ft_ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ft_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as ft_ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ft_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as ft_ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ft_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as ft_ast_roll_10g_stddev,

        -- Added rolling stats for passes
        round(({{ calculate_rolling_avg('passes', 'player_id', 'game_date', 3) }})::numeric, 3) as passes_roll_3g_avg,
        round(({{ calculate_rolling_avg('passes', 'player_id', 'game_date', 5) }})::numeric, 3) as passes_roll_5g_avg,
        round(({{ calculate_rolling_avg('passes', 'player_id', 'game_date', 10) }})::numeric, 3) as passes_roll_10g_avg,
        round(({{ calculate_rolling_stddev('passes', 'player_id', 'game_date', 3) }})::numeric, 3) as passes_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('passes', 'player_id', 'game_date', 5) }})::numeric, 3) as passes_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('passes', 'player_id', 'game_date', 10) }})::numeric, 3) as passes_roll_10g_stddev,

        tracking_boxscore.updated_at

    from tracking_boxscore
)

select *
from final