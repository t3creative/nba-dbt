{{ config(
        schema='intermediate',
        materialized='incremental', 
        unique_key='event_key', 
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'pbp', 'participation']
    )
}}

with synthesized_events as (
    select * from {{ ref('int_pbp__synthesized_events') }}
    /*
    {% if is_incremental() %}
    -- Add incremental logic if materializing as incremental
    -- This depends on how int_pbp__synthesized_events handles updates (e.g., using its updated_at)
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
    */
),

participation_logic as (
    select
        s.synthesized_event_key as event_key,
        s.game_id,
        s.period,
        s.game_clock_seconds_remaining, -- This is numeric seconds remaining in period

        -- Primary event actor
        s.player1_id as primary_player_id,
        s.player1_name as primary_player_name,
        s.player1_team_id as primary_team_id,
        s.event_name_primary as primary_event_name, -- Changed from primary_action_type
        s.event_name_detail as primary_event_detail,

        -- Secondary participants with clear roles (simplified initial logic)
        case
            when s.event_name_primary = 'Made Shot'          -- Event must be a made shot
                 and s.player2_id is not null                -- A second player is listed in the structured data
                 and s.player2_id != s.player1_id            -- This second player is not the shooter
                 and s.player2_name is not null              -- The name of this second player is known (crucial for description check)
                 and (
                        s.description_primary ilike ('%' || s.player2_name || '% AST)')  -- V2 style, e.g., "(Player Name AST)"
                     or s.description_primary ilike ('%(' || s.player2_name || '% assists)') -- V3 style, e.g., "(Player Name assists)"
                 )
            then s.player2_id -- If all conditions met, this player2_id is considered the assister
            else null
        end as assisting_player_id,

        case
            when s.event_name_primary = 'Block' then s.player1_id -- If the primary event is the block itself
            -- Logic for blocker as player2 if primary event is 'Missed Shot' and description indicates block by player2:
            -- WHEN s.event_name_primary = 'Missed Shot' AND s.player2_id IS NOT NULL AND (s.description_primary ILIKE '% BLK %' OR s.description_v2_home ILIKE '% BLK %' OR s.description_v2_away ILIKE '% BLK %') THEN s.player2_id
            else null
        end as defending_player_id, -- Simplified: Blocker on a block event. Further enrichment needed for general defender.

        case
            when s.event_name_primary = 'Rebound' then s.player1_id
            else null
        end as rebounding_player_id,

        -- Event outcomes
        s.shot_made_flag,
        s.points_scored_on_shot as points_scored, -- Renamed from points_scored_on_shot for consistency
        case when s.event_name_primary = 'Turnover' then true else false end as turnover_flag,

        -- Game context features
        s.score_margin,

        case
            when s.period >= 4 and s.game_clock_seconds_remaining <= 300 and abs(coalesce(s.score_margin, 0)) <= 5 then true
            -- Adjust score_margin threshold (e.g. 5 points) and time (300 seconds = 5 minutes) as needed for "clutch time" definition.
            -- OT periods (period > 4) are also clutch by definition if time is low.
            else false
        end as clutch_time_flag,

        (
            case
                when s.period <= 4 then (s.period - 1) * 720 -- 12 minutes * 60 seconds for regular periods
                else (4 * 720) + ((s.period - 5) * 300)     -- 4 regular periods + 5 minutes * 60 seconds for OT periods
            end
        ) +
        (
            (case when s.period <= 4 then 720 else 300 end) - s.game_clock_seconds_remaining
        ) as seconds_elapsed_game,
        
        s.source_synthesis_type, -- To understand event origin
        s.created_at,
        s.updated_at

    from synthesized_events s
)

select * from participation_logic