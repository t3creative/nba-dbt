{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        indexes=[
            {'columns': ['player_game_key'], 'unique': true},
            {'columns': ['player_id', 'season_year']},
            {'columns': ['game_id']}
        ]
    )
}}

/*
Calculates deviations in a player's game-level scoring profile (paint reliance, rim efficiency, rim attempts)
compared to their season averages. Helps identify how a player's scoring approach changes against specific opponents.
*/

with scoring_data as (
    select
        player_game_key,
        player_id,
        game_id,
        team_id,
        season_year,
        game_date,
        opponent_id,
        pct_pts_in_paint
    from {{ ref('int__combined_player_boxscore') }}
    {% if is_incremental() %}
    -- Process games newer than the most recent game date in the table
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

tracking_data as (
    select
        player_game_key,
        player_id,
        game_id,
        def_at_rim_fg_pct,
        def_at_rim_fga,
        -- Need minutes to calculate rates
        min
    from {{ ref('stg__player_tracking_bxsc') }}
    where player_game_key in (select player_game_key from scoring_data) -- Ensure we only process relevant games
),

-- Combine relevant metrics from scoring and tracking
player_game_metrics as (
    select
        s.player_game_key,
        s.player_id,
        s.game_id,
        s.season_year,
        s.game_date,
        s.opponent_id,
        s.pct_pts_in_paint,
        t.def_at_rim_fg_pct,
        t.def_at_rim_fga,
        t.min,
        -- Calculate rim attempt rate per minute played
        coalesce({{ dbt_utils.safe_divide('t.def_at_rim_fga', 't.min') }}, 0) as def_at_rim_fga_rate
    from scoring_data s
    left join tracking_data t on s.player_game_key = t.player_game_key
),

-- Calculate season averages using window functions
player_season_metrics as (
    select
        *,
        avg(pct_pts_in_paint) over (partition by player_id, season_year) as avg_pct_pts_in_paint,
        avg(def_at_rim_fg_pct) over (partition by player_id, season_year) as avg_def_at_rim_fg_pct,
        avg(def_at_rim_fga_rate) over (partition by player_id, season_year) as avg_def_at_rim_fga_rate
    from player_game_metrics
),

final as (
    select
        -- Identifiers
        psm.player_game_key,
        psm.player_id,
        psm.game_id,
        psm.season_year,
        psm.game_date,
        psm.opponent_id,

        -- Raw Game Metrics (for context)
        psm.pct_pts_in_paint,
        psm.def_at_rim_fg_pct,
        psm.def_at_rim_fga_rate,

        -- Deviation Features
        coalesce(psm.pct_pts_in_paint - psm.avg_pct_pts_in_paint, 0) as paint_scoring_reliance_deviation,
        coalesce(psm.def_at_rim_fg_pct - psm.avg_def_at_rim_fg_pct, 0) as rim_finishing_efficiency_deviation,
        coalesce(psm.def_at_rim_fga_rate - psm.avg_def_at_rim_fga_rate, 0) as rim_attempt_rate_deviation

    from player_season_metrics psm
    where psm.season_year >= '2017-18'
)

select * from final 