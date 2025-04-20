{% macro test_time_based_features(model) %}
/*
    Comprehensive test suite for time-based features
    Combines granular testing with efficient validation structure
*/

with feature_data as (
    select * from {{ model }}
),

-- Test 1: Rest Days Validation
rest_days_validation as (
    select
        count(*) as total_records,
        -- Test positive rest days
        sum(case when days_of_rest < 0 then 1 else 0 end) as negative_rest_days,
        -- Test null for first game
        sum(case 
            when days_of_rest is not null 
            and row_number() over (partition by player_id order by game_date) = 1 
            then 1 else 0 
        end) as invalid_first_game_rest,
        -- Test back-to-back binary output
        sum(case when is_back_to_back not in (0, 1) then 1 else 0 end) as invalid_b2b_values
    from feature_data
),

-- Test 2: Rolling Average Validation
rolling_avg_validation as (
    select
        count(*) as total_records,
        -- Test value ranges
        sum(case 
            when pts_rolling_avg_10g is not null 
            and pts_rolling_avg_10g between 0 and 100 
            then 1 else 0 
        end) as valid_rolling_avg_count,
        -- Test null handling
        sum(case 
            when pts_rolling_avg_10g is null and pts is null 
            then 1 else 0 
        end) as proper_null_handling,
        -- Test completeness after minimum periods
        sum(case 
            when row_number() over (partition by player_id order by game_date) > 10
            and pts_rolling_avg_10g is null
            then 1 else 0 
        end) as missing_required_averages
    from feature_data
),

-- Test 3: Recency Weighted Metrics Validation
recency_weighted_validation as (
    select
        count(*) as total_records,
        -- Test value ranges
        sum(case 
            when pts_recency_weighted_linear_10g is not null 
            and pts_recency_weighted_linear_10g between 0 and 100 
            then 1 else 0 
        end) as valid_weighted_count,
        -- Test exponential vs linear weighting difference
        sum(case 
            when pts_recency_weighted_exp_10g != pts_recency_weighted_linear_10g
            then 1 else 0 
        end) as weight_scheme_differences,
        -- Test alpha parameter validity
        case when {{ var('alpha', 0.8) }} between 0 and 1 
        then 0 else 1 end as invalid_alpha
    from feature_data
),

-- Test 4: Team Partitioning Validation
team_partition_validation as (
    select
        count(*) as total_records,
        -- Test team-specific vs general averages
        sum(case 
            when pts_rolling_avg_team_10g != pts_rolling_avg_10g
            and team_id != lag(team_id) over (partition by player_id order by game_date)
            then 1 else 0 
        end) as valid_team_partitioning,
        -- Test home/away completeness
        sum(case 
            when row_number() over (partition by player_id, is_home_game order by game_date) > 5
            and (home_rolling_avg_5 is null or away_rolling_avg_5 is null)
            then 1 else 0 
        end) as invalid_home_away_stats
    from feature_data
),

-- Combine all validation results
validation_results as (
    select
        r.total_records,
        -- Rest days validation
        case when r.negative_rest_days = 0 
            and r.invalid_first_game_rest = 0 
            and r.invalid_b2b_values = 0
        then 1 else 0 end as rest_days_valid,
        
        -- Rolling average validation
        case when ra.valid_rolling_avg_count > 0 
            and ra.missing_required_averages = 0
        then 1 else 0 end as rolling_avg_valid,
        
        -- Recency weighted validation
        case when rw.valid_weighted_count > 0 
            and rw.weight_scheme_differences > 0
            and rw.invalid_alpha = 0
        then 1 else 0 end as recency_weighted_valid,
        
        -- Team partitioning validation
        case when tp.valid_team_partitioning > 0 
            and tp.invalid_home_away_stats = 0
        then 1 else 0 end as team_partitioning_valid
        
    from rest_days_validation r
    cross join rolling_avg_validation ra
    cross join recency_weighted_validation rw
    cross join team_partition_validation tp
)

-- Return failed validations
select * 
from validation_results
where not (
    rest_days_valid = 1 and
    rolling_avg_valid = 1 and
    recency_weighted_valid = 1 and
    team_partitioning_valid = 1
)

{% endmacro %}

-- Individual test macros for specific scenarios
{% macro test_streak_indicator() %}
    select *
    from {{ model }}
    where hot_streak_indicator not in (0, 1)
    or cold_streak_indicator not in (0, 1)
{% endmacro %}

{% macro test_streak_length() %}
    with streak_validation as (
        select 
            player_id,
            game_date,
            hot_streak_indicator,
            cold_streak_indicator,
            lag(pts, 2) over (partition by player_id order by game_date) as pts_2_games_ago,
            lag(pts, 1) over (partition by player_id order by game_date) as pts_previous_game,
            pts as pts_current_game
        from {{ model }}
    )
    select *
    from streak_validation
    where (hot_streak_indicator = 1 and (
        pts_current_game < season_avg_pts or
        pts_previous_game < season_avg_pts or
        pts_2_games_ago < season_avg_pts
    ))
    or (cold_streak_indicator = 1 and (
        pts_current_game > season_avg_pts or
        pts_previous_game > season_avg_pts or
        pts_2_games_ago > season_avg_pts
    ))
{% endmacro %} 