{{ config(
    schema='intermediate',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'milestones', 'outliers', 'features'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

/*
  NBA Milestone Games Detection System
  
  Detects various types of milestone and outlier performances:
  - Traditional statistical milestones (30+ pts, 10+ ast, etc.)
  - Career achievement milestones (new career highs)
  - Statistical rarity indicators (performance vs career norms)
  - Multi-category excellence games
  - Breakout performance detection
  
  Temporal Safety: All features use only historical data available at game time
*/

{% set traditional_thresholds = {
    'pts': [20, 25, 30, 35, 40, 45, 50],
    'ast': [5, 8, 10, 12, 15, 18, 20],
    'reb': [8, 10, 12, 15, 18, 20],
    'stl': [3, 4, 5, 6, 7, 8],
    'blk': [3, 4, 5, 6, 7, 8],
    'fg3m': [4, 5, 6, 7, 8, 9, 10]
} %}

{% set elite_thresholds = {
    'pts': [60, 70, 80],
    'ast': [25, 30],
    'reb': [25, 30],
    'stl': [10, 12],
    'blk': [10, 12],
    'fg3m': [12, 15]
} %}

with career_stats_base as (
    select *
    from {{ ref('int_player__career_to_date_stats_v2') }}
    {% if is_incremental() %}
    where game_date > (
        select coalesce(max(game_date), '1900-01-01'::date) 
        from {{ this }}
    )
    {% endif %}
),

-- Calculate statistical context for rarity detection
player_statistical_context as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_id,
        season_year,
        
        -- Current game stats
        min,
        pts, ast, reb, stl, blk, fg3m,
        fgm, fga, ftm, fta, tov,
        
        -- Career context (up to this game)
        career_games_to_date,
        career_ppg_to_date,
        career_apg_to_date,
        career_rpg_to_date,
        career_stl_pg_to_date,
        career_blk_pg_to_date,
        career_3pm_pg_to_date,
        
        -- Career highs (up to this game)
        career_high_pts_to_date,
        career_high_ast_to_date,
        career_high_reb_to_date,
        career_high_stl_to_date,
        career_high_blk_to_date,
        career_high_fg3m_to_date,
        
        -- New career high flags
        is_new_career_high_pts,
        is_new_career_high_ast,
        is_new_career_high_reb,
        is_new_career_high_stl,
        is_new_career_high_blk,
        is_new_career_high_fg3m,
        
        -- Calculate performance vs career averages (only if min games played)
        case 
            when career_games_to_date >= 10 and career_ppg_to_date > 0 
            then round((pts / nullif(career_ppg_to_date, 0))::numeric, 2)
            else null 
        end as pts_vs_career_avg_ratio,
        
        case 
            when career_games_to_date >= 10 and career_apg_to_date > 0 
            then round((ast / nullif(career_apg_to_date, 0))::numeric, 2)
            else null 
        end as ast_vs_career_avg_ratio,
        
        case 
            when career_games_to_date >= 10 and career_rpg_to_date > 0 
            then round((reb / nullif(career_rpg_to_date, 0))::numeric, 2)
            else null 
        end as reb_vs_career_avg_ratio

    from career_stats_base
),

-- Traditional milestone detection
traditional_milestones as (
    select
        player_game_key,
        
        -- Points milestones
        {% for threshold in traditional_thresholds['pts'] %}
        case when pts >= {{ threshold }} then 1 else 0 end as pts_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Assists milestones  
        {% for threshold in traditional_thresholds['ast'] %}
        case when ast >= {{ threshold }} then 1 else 0 end as ast_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Rebounds milestones
        {% for threshold in traditional_thresholds['reb'] %}
        case when reb >= {{ threshold }} then 1 else 0 end as reb_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Steals milestones
        {% for threshold in traditional_thresholds['stl'] %}
        case when stl >= {{ threshold }} then 1 else 0 end as stl_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Blocks milestones
        {% for threshold in traditional_thresholds['blk'] %}
        case when blk >= {{ threshold }} then 1 else 0 end as blk_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Three-pointers milestones
        {% for threshold in traditional_thresholds['fg3m'] %}
        case when fg3m >= {{ threshold }} then 1 else 0 end as fg3m_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Elite performance flags
        {% for stat, thresholds in elite_thresholds.items() %}
        {% for threshold in thresholds %}
        case when {{ stat }} >= {{ threshold }} then 1 else 0 end as {{ stat }}_{{ threshold }}_elite_game,
        {% endfor %}
        {% endfor %}
        
        -- Multi-category milestone combinations
        case when pts >= 20 and reb >= 10 then 1 else 0 end as double_double_20_10_game,
        case when pts >= 25 and reb >= 10 then 1 else 0 end as double_double_25_10_game,
        case when pts >= 20 and ast >= 10 then 1 else 0 end as double_double_20_10_ast_game,
        case when pts >= 25 and ast >= 10 then 1 else 0 end as double_double_25_10_ast_game,
        case when reb >= 10 and ast >= 10 then 1 else 0 end as double_double_reb_ast_game,
        
        -- Triple-double indicators
        case when 
            (case when pts >= 10 then 1 else 0 end) +
            (case when reb >= 10 then 1 else 0 end) +
            (case when ast >= 10 then 1 else 0 end) +
            (case when stl >= 10 then 1 else 0 end) +
            (case when blk >= 10 then 1 else 0 end) >= 3 
        then 1 else 0 end as triple_double_game,
        
        -- Near triple-double (2 categories in double digits + 1 category 8+)
        case when 
            (case when pts >= 10 then 1 else 0 end) +
            (case when reb >= 10 then 1 else 0 end) +
            (case when ast >= 10 then 1 else 0 end) +
            (case when stl >= 10 then 1 else 0 end) +
            (case when blk >= 10 then 1 else 0 end) >= 2
            and (pts >= 8 or reb >= 8 or ast >= 8 or stl >= 8 or blk >= 8)
        then 1 else 0 end as near_triple_double_game

    from player_statistical_context
),

-- Performance rarity and outlier detection
performance_rarity as (
    select
        player_game_key,
        
        -- Career performance percentiles (only for players with 20+ games)
        case 
            when career_games_to_date >= 20 
            then ntile(100) over (
                partition by player_id 
                order by pts 
                rows between unbounded preceding and current row
            )
            else null 
        end as pts_career_percentile,
        
        case 
            when career_games_to_date >= 20 
            then ntile(100) over (
                partition by player_id 
                order by ast 
                rows between unbounded preceding and current row
            )
            else null 
        end as ast_career_percentile,
        
        case 
            when career_games_to_date >= 20 
            then ntile(100) over (
                partition by player_id 
                order by reb 
                rows between unbounded preceding and current row
            )
            else null 
        end as reb_career_percentile,
        
        -- Outlier performance flags (top 5% of career games)
        case 
            when career_games_to_date >= 20 and 
                 ntile(100) over (
                     partition by player_id 
                     order by pts 
                     rows between unbounded preceding and current row
                 ) >= 95
            then 1 else 0 
        end as pts_career_outlier_game,
        
        case 
            when career_games_to_date >= 20 and 
                 ntile(100) over (
                     partition by player_id 
                     order by ast 
                     rows between unbounded preceding and current row
                 ) >= 95
            then 1 else 0 
        end as ast_career_outlier_game,
        
        case 
            when career_games_to_date >= 20 and 
                 ntile(100) over (
                     partition by player_id 
                     order by reb 
                     rows between unbounded preceding and current row
                 ) >= 95
            then 1 else 0 
        end as reb_career_outlier_game,
        
        -- Multi-stat outlier (outlier in 2+ categories)
        case 
            when (
                case 
                    when career_games_to_date >= 20 and 
                         ntile(100) over (
                             partition by player_id 
                             order by pts 
                             rows between unbounded preceding and current row
                         ) >= 90
                    then 1 else 0 
                end +
                case 
                    when career_games_to_date >= 20 and 
                         ntile(100) over (
                             partition by player_id 
                             order by ast 
                             rows between unbounded preceding and current row
                         ) >= 90
                    then 1 else 0 
                end +
                case 
                    when career_games_to_date >= 20 and 
                         ntile(100) over (
                             partition by player_id 
                             order by reb 
                             rows between unbounded preceding and current row
                         ) >= 90
                    then 1 else 0 
                end
            ) >= 2
            then 1 else 0 
        end as multi_stat_career_outlier_game,
        
        -- Performance vs career average ratios
        pts_vs_career_avg_ratio,
        ast_vs_career_avg_ratio,
        reb_vs_career_avg_ratio,
        
        -- Breakout performance indicators
        case 
            when pts_vs_career_avg_ratio >= 2.0 and career_games_to_date >= 10 
            then 1 else 0 
        end as pts_breakout_game,
        
        case 
            when ast_vs_career_avg_ratio >= 2.0 and career_games_to_date >= 10 
            then 1 else 0 
        end as ast_breakout_game,
        
        case 
            when reb_vs_career_avg_ratio >= 2.0 and career_games_to_date >= 10 
            then 1 else 0 
        end as reb_breakout_game

    from player_statistical_context
),

-- Comprehensive milestone summary
milestone_summary as (
    select
        psc.player_game_key,
        psc.player_id,
        psc.player_name,
        psc.game_id,
        psc.game_date,
        psc.season_id,
        psc.season_year,
        
        -- Game stats
        psc.min,
        psc.pts, psc.ast, psc.reb, psc.stl, psc.blk, psc.fg3m,
        psc.fgm, psc.fga, psc.ftm, psc.fta, psc.tov,
        
        -- Career context
        psc.career_games_to_date,
        psc.career_ppg_to_date,
        psc.career_apg_to_date,
        psc.career_rpg_to_date,
        
        -- Career achievement flags
        psc.is_new_career_high_pts,
        psc.is_new_career_high_ast,
        psc.is_new_career_high_reb,
        psc.is_new_career_high_stl,
        psc.is_new_career_high_blk,
        psc.is_new_career_high_fg3m,
        
        -- Any new career high flag
        case when (
            psc.is_new_career_high_pts + psc.is_new_career_high_ast + 
            psc.is_new_career_high_reb + psc.is_new_career_high_stl + 
            psc.is_new_career_high_blk + psc.is_new_career_high_fg3m
        ) > 0 then 1 else 0 end as any_new_career_high_game,
        
        -- Traditional milestone flags (explicit selection to avoid column duplication)
        {% for threshold in traditional_thresholds['pts'] %}
        tm.pts_{{ threshold }}_plus_game,
        {% endfor %}
        {% for threshold in traditional_thresholds['ast'] %}
        tm.ast_{{ threshold }}_plus_game,
        {% endfor %}
        {% for threshold in traditional_thresholds['reb'] %}
        tm.reb_{{ threshold }}_plus_game,
        {% endfor %}
        {% for threshold in traditional_thresholds['stl'] %}
        tm.stl_{{ threshold }}_plus_game,
        {% endfor %}
        {% for threshold in traditional_thresholds['blk'] %}
        tm.blk_{{ threshold }}_plus_game,
        {% endfor %}
        {% for threshold in traditional_thresholds['fg3m'] %}
        tm.fg3m_{{ threshold }}_plus_game,
        {% endfor %}
        
        -- Elite performance flags
        {% for stat, thresholds in elite_thresholds.items() %}
        {% for threshold in thresholds %}
        tm.{{ stat }}_{{ threshold }}_elite_game,
        {% endfor %}
        {% endfor %}
        
        -- Multi-category combinations
        tm.double_double_20_10_game,
        tm.double_double_25_10_game,
        tm.double_double_20_10_ast_game,
        tm.double_double_25_10_ast_game,
        tm.double_double_reb_ast_game,
        tm.triple_double_game,
        tm.near_triple_double_game,
        
        -- Performance rarity metrics (from performance_rarity CTE)
        pr.pts_career_percentile,
        pr.ast_career_percentile,
        pr.reb_career_percentile,
        pr.pts_career_outlier_game,
        pr.ast_career_outlier_game,
        pr.reb_career_outlier_game,
        pr.multi_stat_career_outlier_game,
        pr.pts_vs_career_avg_ratio,
        pr.ast_vs_career_avg_ratio,
        pr.reb_vs_career_avg_ratio,
        pr.pts_breakout_game,
        pr.ast_breakout_game,
        pr.reb_breakout_game,
        
        -- Comprehensive outlier score (0-10 scale)
        (
            -- Career high bonuses (2 points each)
            (psc.is_new_career_high_pts * 2) +
            (psc.is_new_career_high_ast * 2) +
            (psc.is_new_career_high_reb * 2) +
            
            -- Traditional milestone bonuses (1 point each for higher thresholds)
            tm.pts_30_plus_game + tm.pts_40_plus_game + tm.pts_50_plus_game +
            tm.ast_10_plus_game + tm.ast_15_plus_game + tm.ast_20_plus_game +
            tm.reb_15_plus_game + tm.reb_20_plus_game +
            
            -- Multi-category bonuses (2 points each)
            (tm.double_double_25_10_game * 2) +
            (tm.triple_double_game * 3) +
            
            -- Statistical rarity bonuses (1-2 points)
            pr.pts_career_outlier_game + pr.ast_career_outlier_game + pr.reb_career_outlier_game +
            (pr.multi_stat_career_outlier_game * 2) +
            
            -- Breakout performance bonuses (1 point each)
            pr.pts_breakout_game + pr.ast_breakout_game + pr.reb_breakout_game
        ) as milestone_outlier_score,
        
        -- Is this a "signature" game? (high milestone score or multiple rare achievements)
        case 
            when (
                -- High outlier score
                (
                    (psc.is_new_career_high_pts * 2) +
                    (psc.is_new_career_high_ast * 2) +
                    (psc.is_new_career_high_reb * 2) +
                    tm.pts_30_plus_game + tm.pts_40_plus_game + tm.pts_50_plus_game +
                    tm.ast_10_plus_game + tm.ast_15_plus_game + tm.ast_20_plus_game +
                    tm.reb_15_plus_game + tm.reb_20_plus_game +
                    (tm.double_double_25_10_game * 2) +
                    (tm.triple_double_game * 3) +
                    pr.pts_career_outlier_game + pr.ast_career_outlier_game + pr.reb_career_outlier_game +
                    (pr.multi_stat_career_outlier_game * 2) +
                    pr.pts_breakout_game + pr.ast_breakout_game + pr.reb_breakout_game
                ) >= 5
                -- OR specific rare combinations
                or tm.triple_double_game = 1
                or tm.pts_50_plus_game = 1
                or pr.multi_stat_career_outlier_game = 1
                or (psc.is_new_career_high_pts = 1 and tm.pts_30_plus_game = 1)
            )
            then 1 else 0 
        end as signature_performance_game

    from player_statistical_context psc
    left join traditional_milestones tm 
        on psc.player_game_key = tm.player_game_key
    left join performance_rarity pr 
        on psc.player_game_key = pr.player_game_key
)

select * from milestone_summary