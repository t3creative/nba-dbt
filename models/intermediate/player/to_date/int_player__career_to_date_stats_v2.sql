{{ config(
    schema='intermediate',
    materialized='incremental',
    unique_key='player_game_key',
    on_schema_change='sync_all_columns',
    indexes=[
        {'columns': ['player_game_key']},
        {'columns': ['game_id', 'player_id']}
    ]
) }}

with
-- Get source game logs for authoritative game dates
game_logs as (
    select distinct
        game_id,
        game_date,
        season_id
    from {{ ref('stg__game_logs_league') }}
),

-- Get player box scores
source_data as (
    select
        pbs.player_game_key,
        pbs.player_id,
        pbs.game_id,
        pbs.team_id,
        pbs.first_name,
        pbs.family_name,
        pbs.min,
        pbs.pts,
        pbs.ast,
        pbs.reb,
        pbs.stl,
        pbs.blk,
        pbs.fgm,
        pbs.fga,
        pbs.fg3m,
        pbs.fg3a,
        pbs.fta,
        pbs.ftm,
        pbs.tov,
        gl.game_date,
        gl.season_id,
        pbs.season_year
    from {{ ref('int__player_bxsc_traditional') }} pbs
    inner join game_logs gl on pbs.game_id = gl.game_id
),

-- For incremental models, get previous career stats to use as starting point
{% if is_incremental() %}
previous_career_stats as (
    select
        player_id,
        max(career_total_min) as prev_career_total_min,
        max(career_total_pts) as prev_career_total_pts,
        max(career_total_ast) as prev_career_total_ast,
        max(career_total_reb) as prev_career_total_reb,
        max(career_total_stl) as prev_career_total_stl,
        max(career_total_blk) as prev_career_total_blk,
        max(career_total_fgm) as prev_career_total_fgm,
        max(career_total_fga) as prev_career_total_fga,
        max(career_total_fg3m) as prev_career_total_fg3m,
        max(career_total_fg3a) as prev_career_total_fg3a,
        max(career_total_ftm) as prev_career_total_ftm,
        max(career_total_fta) as prev_career_total_fta,
        max(career_total_tov) as prev_career_total_tov,
        max(career_games_to_date) as prev_career_games, -- Renamed from career_total_games
        max(career_high_min_to_date) as prev_career_high_min,
        max(career_high_pts_to_date) as prev_career_high_pts,
        max(career_high_ast_to_date) as prev_career_high_ast,
        max(career_high_reb_to_date) as prev_career_high_reb,
        max(career_high_stl_to_date) as prev_career_high_stl,
        max(career_high_blk_to_date) as prev_career_high_blk,
        max(career_high_fgm_to_date) as prev_career_high_fgm,
        max(career_high_fga_to_date) as prev_career_high_fga,
        max(career_high_fg3m_to_date) as prev_career_high_fg3m,
        max(career_high_fg3a_to_date) as prev_career_high_fg3a,
        max(career_high_ftm_to_date) as prev_career_high_ftm,
        max(career_high_fta_to_date) as prev_career_high_fta,
        max(career_high_tov_to_date) as prev_career_high_tov
    from {{ this }}
    group by player_id
),
{% endif %}

-- Calculate current period stats
current_period_stats as (
    select
        player_game_key,
        player_id,
        first_name || ' ' || family_name as player_name,
        game_id,
        game_date,
        season_id,
        season_year,
        min,
        pts,
        ast,
        reb,
        stl,
        blk,
        fgm,
        fga,
        fg3m,
        fg3a,
        fta,
        ftm,
        tov,
        row_number() over (partition by player_id order by game_date, game_id) as game_seq
    from source_data
)

select
    cps.player_game_key,
    cps.player_id,
    cps.player_name,
    cps.game_id,
    cps.game_date,
    cps.season_id,
    cps.season_year,
    cps.min,
    cps.pts,
    cps.ast,
    cps.reb,
    cps.stl,
    cps.blk,
    cps.fgm,
    cps.fga,
    cps.fg3m,
    cps.fg3a,
    cps.ftm,
    cps.fta,
    cps.tov,
    {% if is_incremental() %}
        cps.min + coalesce(pcs.prev_career_total_min, 0) as career_total_min,
        cps.pts + coalesce(pcs.prev_career_total_pts, 0) as career_total_pts,
        cps.ast + coalesce(pcs.prev_career_total_ast, 0) as career_total_ast,
        cps.reb + coalesce(pcs.prev_career_total_reb, 0) as career_total_reb,
        cps.stl + coalesce(pcs.prev_career_total_stl, 0) as career_total_stl,
        cps.blk + coalesce(pcs.prev_career_total_blk, 0) as career_total_blk,
        cps.fgm + coalesce(pcs.prev_career_total_fgm, 0) as career_total_fgm,
        cps.fga + coalesce(pcs.prev_career_total_fga, 0) as career_total_fga,
        cps.fg3m + coalesce(pcs.prev_career_total_fg3m, 0) as career_total_fg3m,
        cps.fg3a + coalesce(pcs.prev_career_total_fg3a, 0) as career_total_fg3a,
        cps.ftm + coalesce(pcs.prev_career_total_ftm, 0) as career_total_ftm,
        cps.fta + coalesce(pcs.prev_career_total_fta, 0) as career_total_fta,
        cps.tov + coalesce(pcs.prev_career_total_tov, 0) as career_total_tov,
        cps.game_seq + coalesce(pcs.prev_career_games, 0) as career_games_to_date,

        (cps.min + coalesce(pcs.prev_career_total_min, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_min_pg_to_date,
        (cps.pts + coalesce(pcs.prev_career_total_pts, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_ppg_to_date,
        (cps.ast + coalesce(pcs.prev_career_total_ast, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_apg_to_date,
        (cps.reb + coalesce(pcs.prev_career_total_reb, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_rpg_to_date,
        (cps.stl + coalesce(pcs.prev_career_total_stl, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_stl_pg_to_date,
        (cps.blk + coalesce(pcs.prev_career_total_blk, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_blk_pg_to_date,
        (cps.fgm + coalesce(pcs.prev_career_total_fgm, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_fgm_pg_to_date,
        (cps.fga + coalesce(pcs.prev_career_total_fga, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_fga_pg_to_date,
        (cps.fg3m + coalesce(pcs.prev_career_total_fg3m, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_3pm_pg_to_date,
        (cps.ftm + coalesce(pcs.prev_career_total_ftm, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_ftm_pg_to_date,
        (cps.fta + coalesce(pcs.prev_career_total_fta, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_fta_pg_to_date,
        (cps.tov + coalesce(pcs.prev_career_total_tov, 0)) / nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_tov_pg_to_date,

        greatest(cps.min, coalesce(pcs.prev_career_high_min, 0)) as career_high_min_to_date,
        greatest(cps.pts, coalesce(pcs.prev_career_high_pts, 0)) as career_high_pts_to_date,
        greatest(cps.ast, coalesce(pcs.prev_career_high_ast, 0)) as career_high_ast_to_date,
        greatest(cps.reb, coalesce(pcs.prev_career_high_reb, 0)) as career_high_reb_to_date,
        greatest(cps.stl, coalesce(pcs.prev_career_high_stl, 0)) as career_high_stl_to_date,
        greatest(cps.blk, coalesce(pcs.prev_career_high_blk, 0)) as career_high_blk_to_date,
        greatest(cps.fgm, coalesce(pcs.prev_career_high_fgm, 0)) as career_high_fgm_to_date,
        greatest(cps.fga, coalesce(pcs.prev_career_high_fga, 0)) as career_high_fga_to_date,
        greatest(cps.fg3m, coalesce(pcs.prev_career_high_fg3m, 0)) as career_high_fg3m_to_date,
        greatest(cps.fg3a, coalesce(pcs.prev_career_high_fg3a, 0)) as career_high_fg3a_to_date,
        greatest(cps.ftm, coalesce(pcs.prev_career_high_ftm, 0)) as career_high_ftm_to_date,
        greatest(cps.fta, coalesce(pcs.prev_career_high_fta, 0)) as career_high_fta_to_date,
        greatest(cps.tov, coalesce(pcs.prev_career_high_tov, 0)) as career_high_tov_to_date,

        case when cps.min > coalesce(pcs.prev_career_high_min, 0) then 1 else 0 end as is_new_career_high_min,
        case when cps.pts > coalesce(pcs.prev_career_high_pts, 0) then 1 else 0 end as is_new_career_high_pts,
        case when cps.ast > coalesce(pcs.prev_career_high_ast, 0) then 1 else 0 end as is_new_career_high_ast,
        case when cps.reb > coalesce(pcs.prev_career_high_reb, 0) then 1 else 0 end as is_new_career_high_reb,
        case when cps.stl > coalesce(pcs.prev_career_high_stl, 0) then 1 else 0 end as is_new_career_high_stl,
        case when cps.blk > coalesce(pcs.prev_career_high_blk, 0) then 1 else 0 end as is_new_career_high_blk,
        case when cps.fgm > coalesce(pcs.prev_career_high_fgm, 0) then 1 else 0 end as is_new_career_high_fgm,
        case when cps.fga > coalesce(pcs.prev_career_high_fga, 0) then 1 else 0 end as is_new_career_high_fga,
        case when cps.fg3m > coalesce(pcs.prev_career_high_fg3m, 0) then 1 else 0 end as is_new_career_high_fg3m,
        case when cps.fg3a > coalesce(pcs.prev_career_high_fg3a, 0) then 1 else 0 end as is_new_career_high_fg3a,
        case when cps.ftm > coalesce(pcs.prev_career_high_ftm, 0) then 1 else 0 end as is_new_career_high_ftm,
        case when cps.fta > coalesce(pcs.prev_career_high_fta, 0) then 1 else 0 end as is_new_career_high_fta,
        case when cps.tov > coalesce(pcs.prev_career_high_tov, 0) then 1 else 0 end as is_new_career_high_tov

    {% else %}
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.min', 'sum') }} as career_total_min,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.pts', 'sum') }} as career_total_pts,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ast', 'sum') }} as career_total_ast,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.reb', 'sum') }} as career_total_reb,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.stl', 'sum') }} as career_total_stl,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.blk', 'sum') }} as career_total_blk,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fgm', 'sum') }} as career_total_fgm,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fga', 'sum') }} as career_total_fga,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3m', 'sum') }} as career_total_fg3m,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3a', 'sum') }} as career_total_fg3a,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ftm', 'sum') }} as career_total_ftm,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fta', 'sum') }} as career_total_fta,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.tov', 'sum') }} as career_total_tov,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', '1', 'count') }} as career_games_to_date,

        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.min', 'avg') }}, 2) as career_min_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.pts', 'avg') }}, 2) as career_ppg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ast', 'avg') }}, 2) as career_apg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.reb', 'avg') }}, 2) as career_rpg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.stl', 'avg') }}, 2) as career_stl_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.blk', 'avg') }}, 2) as career_blk_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3m', 'avg') }}, 2) as career_3pm_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fgm', 'avg') }}, 2) as career_fgm_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fga', 'avg') }}, 2) as career_fga_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ftm', 'avg') }}, 2) as career_ftm_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fta', 'avg') }}, 2) as career_fta_pg_to_date,
        round({{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.tov', 'avg') }}, 2) as career_tov_pg_to_date,

        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.min', 'max') }} as career_high_min_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.pts', 'max') }} as career_high_pts_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ast', 'max') }} as career_high_ast_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.reb', 'max') }} as career_high_reb_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.stl', 'max') }} as career_high_stl_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.blk', 'max') }} as career_high_blk_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fgm', 'max') }} as career_high_fgm_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fga', 'max') }} as career_high_fga_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3m', 'max') }} as career_high_fg3m_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3a', 'max') }} as career_high_fg3a_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ftm', 'max') }} as career_high_ftm_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fta', 'max') }} as career_high_fta_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.tov', 'max') }} as career_high_tov_to_date,

        case when cps.min = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.min', 'max') }}
             and cps.min > coalesce(max(cps.min) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_min,
        case when cps.pts = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.pts', 'max') }}
             and cps.pts > coalesce(max(cps.pts) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_pts,
        case when cps.ast = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ast', 'max') }}
             and cps.ast > coalesce(max(cps.ast) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_ast,
        case when cps.reb = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.reb', 'max') }}
             and cps.reb > coalesce(max(cps.reb) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_reb,
        case when cps.stl = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.stl', 'max') }}
             and cps.stl > coalesce(max(cps.stl) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_stl,
        case when cps.blk = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.blk', 'max') }}
             and cps.blk > coalesce(max(cps.blk) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_blk,
        case when cps.fgm = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fgm', 'max') }}
             and cps.fgm > coalesce(max(cps.fgm) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_fgm,
        case when cps.fga = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fga', 'max') }}
             and cps.fga > coalesce(max(cps.fga) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_fga,
        case when cps.fg3m = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3m', 'max') }}
             and cps.fg3m > coalesce(max(cps.fg3m) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_fg3m,
        case when cps.fg3a = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fg3a', 'max') }}
             and cps.fg3a > coalesce(max(cps.fg3a) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_fg3a,
        case when cps.ftm = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.ftm', 'max') }}
             and cps.ftm > coalesce(max(cps.ftm) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_ftm,
        case when cps.fta = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.fta', 'max') }}
             and cps.fta > coalesce(max(cps.fta) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_fta,
        case when cps.tov = {{ career_stat('cps.player_id', 'cps.game_date, cps.game_id', 'cps.tov', 'max') }}
             and cps.tov > coalesce(max(cps.tov) over (partition by cps.player_id order by cps.game_date, cps.game_id rows between unbounded preceding and 1 preceding), 0)
             then 1 else 0 end as is_new_career_high_tov
    {% endif %}
from current_period_stats cps
{% if is_incremental() %}
left join previous_career_stats pcs
    on cps.player_id = pcs.player_id
{% endif %}
