{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'scoring'],
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

with player_scoring_base_data as (
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

        -- Base counts for calculations
        min,
        possessions,
        fga, -- Total field goals attempted
        pts, -- Total points
        fgm, -- Total field goals made

        -- Scoring Percentages from int_player__combined_boxscore (originally from stg__player_scoring_bxsc)
        pct_fga_2pt,
        pct_fga_3pt,
        pct_pts_2pt,
        pct_pts_midrange_2pt,
        pct_pts_3pt,
        -- pct_pts_fastbreak, -- Will use direct fastbreak_pts
        pct_pts_ft,
        -- pct_pts_off_tov, -- Will use direct pts_off_tov
        -- pct_pts_in_paint, -- Will use direct pts_in_paint
        pct_assisted_fgm,
        pct_unassisted_fgm,

        -- Direct Scoring Counts from int_player__combined_boxscore (originally from stg__player_misc_bxsc)
        fastbreak_pts,
        pts_off_tov,
        pts_in_paint,

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

calculated_scoring_values as (
    select
        *,
        -- Calculate absolute values from percentages
        round((fga * pct_fga_2pt)::numeric, 3) as fga_2pt_calc,
        round((fga * pct_fga_3pt)::numeric, 3) as fga_3pt_calc,
        round((pts * pct_pts_2pt)::numeric, 3) as pts_2pt_calc,
        round((pts * pct_pts_midrange_2pt)::numeric, 3) as pts_midrange_2pt_calc,
        round((pts * pct_pts_3pt)::numeric, 3) as pts_3pt_calc,
        round((pts * pct_pts_ft)::numeric, 3) as pts_ft_calc,
        round((fgm * pct_assisted_fgm)::numeric, 3) as assisted_fgm_calc,
        round((fgm * pct_unassisted_fgm)::numeric, 3) as unassisted_fgm_calc
    from player_scoring_base_data
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

        -- Per Minute Ratios
        case when min > 0 then round((fga_2pt_calc / min)::numeric, 3) else 0 end as fga_2pt_per_min,
        case when min > 0 then round((fga_3pt_calc / min)::numeric, 3) else 0 end as fga_3pt_per_min,
        case when min > 0 then round((pts_2pt_calc / min)::numeric, 3) else 0 end as pts_2pt_per_min,
        case when min > 0 then round((pts_midrange_2pt_calc / min)::numeric, 3) else 0 end as pts_midrange_2pt_per_min,
        case when min > 0 then round((pts_3pt_calc / min)::numeric, 3) else 0 end as pts_3pt_per_min,
        case when min > 0 then round((pts_ft_calc / min)::numeric, 3) else 0 end as pts_ft_per_min,
        case when min > 0 then round((assisted_fgm_calc / min)::numeric, 3) else 0 end as assisted_fgm_per_min,
        case when min > 0 then round((unassisted_fgm_calc / min)::numeric, 3) else 0 end as unassisted_fgm_per_min,
        case when min > 0 then round((fastbreak_pts / min)::numeric, 3) else 0 end as fastbreak_pts_per_min,
        case when min > 0 then round((pts_off_tov / min)::numeric, 3) else 0 end as pts_off_tov_per_min,
        case when min > 0 then round((pts_in_paint / min)::numeric, 3) else 0 end as pts_in_paint_per_min,

        -- Per 100 Possessions Ratios
        case when possessions > 0 and min > 0 then round(((fga_2pt_calc / possessions) * 100)::numeric, 3) else 0 end as fga_2pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((fga_3pt_calc / possessions) * 100)::numeric, 3) else 0 end as fga_3pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_2pt_calc / possessions) * 100)::numeric, 3) else 0 end as pts_2pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_midrange_2pt_calc / possessions) * 100)::numeric, 3) else 0 end as pts_midrange_2pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_3pt_calc / possessions) * 100)::numeric, 3) else 0 end as pts_3pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_ft_calc / possessions) * 100)::numeric, 3) else 0 end as pts_ft_per_100_poss,
        case when possessions > 0 and min > 0 then round(((assisted_fgm_calc / possessions) * 100)::numeric, 3) else 0 end as assisted_fgm_per_100_poss,
        case when possessions > 0 and min > 0 then round(((unassisted_fgm_calc / possessions) * 100)::numeric, 3) else 0 end as unassisted_fgm_per_100_poss,
        case when possessions > 0 and min > 0 then round(((fastbreak_pts / possessions) * 100)::numeric, 3) else 0 end as fastbreak_pts_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_off_tov / possessions) * 100)::numeric, 3) else 0 end as pts_off_tov_per_100_poss,
        case when possessions > 0 and min > 0 then round(((pts_in_paint / possessions) * 100)::numeric, 3) else 0 end as pts_in_paint_per_100_poss,
        
        updated_at
    from calculated_scoring_values
)

select *
from final
