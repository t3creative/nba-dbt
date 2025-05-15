{{ config(
        schema='intermediate',
        materialized='incremental',
        unique_key='synthesized_event_key',
        on_schema_change='sync_all_columns'
    )
}}

/*
IMPORTANT PRE-REQUISITES FOR `int__combined_events.sql` FOR OPTIMAL SYNTHESIS:
1. V2 Event Timing: Ensure `int__combined_events` provides a numeric `seconds_remaining_in_period` for v2 events.
   Currently, this model will parse `game_clock_time` (e.g., "12:00") from v2 events.
2. V3 Event Timing: Ensure `int__combined_events` provides a numeric `seconds_remaining_in_period` for v3 events,
   parsed from the raw v3 clock (e.g., "PT12M00.00S"). Currently, `game_clock_time` is NULL for v3 in `int__combined_events`.
   This model will use a placeholder `v3_event_seconds_remaining_placeholder` which you'll need to replace
   with the actual field from `int__combined_events` once available.
3. V3 Player Information: Ensure `int__combined_events` populates `player1_id`, `player1_name`, `player1_team_id`
   for v3 events using `personId` from the raw v3 source. Currently, these are NULL for v3 events
   in `int__combined_events`, limiting the richness of v3_only events.
*/

with combined_events_source as (
    select *
    from {{ ref('int_pbp__combined_events') }}
    {% if is_incremental() %}
    -- Assuming 'updated_at' or a reliable incremental key exists in int__combined_events
    -- If not, this incremental logic might need adjustment based on how int__combined_events is managed.
    -- Using game_date as a proxy if int__combined_events is partitioned by it and reprocessed by game_date.
    where game_date >= (select max(game_date) - interval '3 days' from {{ this }})
    {% endif %}
),

v2_events_parsed as (
    select
        *,
        -- Parse v2 game_clock_time ("MM:SS") to seconds_remaining_in_period
        case
            when game_clock_time is not null and position(':' in game_clock_time) > 0 then
                split_part(game_clock_time, ':', 1)::integer * 60 + split_part(game_clock_time, ':', 2)::integer
            else null
        end as v2_event_seconds_remaining,
        -- Parse v2 score ("H - A") to numeric home_score and away_score
        case
            when score is not null and position(' - ' in score) > 0 then
                case
                    when split_part(score, ' - ', 1) ~ '^[0-9]+$' then split_part(score, ' - ', 1)::integer
                    else null
                end
            else null
        end as v2_parsed_home_score,
        case
            when score is not null and position(' - ' in score) > 0 then
                case
                    when split_part(score, ' - ', 2) ~ '^[0-9]+$' then split_part(score, ' - ', 2)::integer
                    else null
                end
            else null
        end as v2_parsed_away_score
    from combined_events_source
    where source_version = 'v2'
),

v3_events_prepared as (
    select
        *,
        -- Placeholder for v3 seconds remaining. int__combined_events.sql needs to provide this.
        -- For example, if int__combined_events adds a field `v3_parsed_seconds_remaining_from_raw_clock`
        null::numeric as v3_event_seconds_remaining_placeholder -- Replace with actual field
    from combined_events_source
    where source_version = 'v3'
),

joined_events as (
    select
        v2.*,
        v3.game_id as v3_game_id, -- To check for match
        v3.action_type as v3_action_type,
        v3.description as v3_description,
        v3.home_score as v3_home_score,
        v3.away_score as v3_away_score,
        v3.sub_type as v3_sub_type,
        v3.shot_result as v3_shot_result,
        v3.points as v3_points,
        v3.shot_value as v3_shot_value,
        v3.shot_zone as v3_shot_zone,
        v3.shot_distance as v3_shot_distance,
        v3.shot_x as v3_shot_x,
        v3.shot_y as v3_shot_y,
        v3.location as v3_location,
        v3.v3_event_seconds_remaining_placeholder,
        v3.created_at as v3_created_at, -- Added v3 created_at
        v3.updated_at as v3_updated_at  -- Added v3 updated_at
        -- Add other v3 fields from v3_events_prepared as needed for coalescing
    from v2_events_parsed v2
    left join v3_events_prepared v3
        on v2.game_id = v3.game_id
        and v2.period = v3.period
        and v2.event_number = v3.event_number -- Assumes event_number is actionNumber for v3 in combined_events
),

synthesized_from_v2_base as (
    select
        -- Event Identifiers
        je.game_id,
        je.period,
        je.event_number,
        je.game_id || '_' || je.period || '_' || je.event_number as synthesized_event_key,
        case
            when je.v3_game_id is not null then 'v2_enriched_by_v3'
            else 'v2_only'
        end as source_synthesis_type,
        je.game_date,

        -- Timing
        coalesce(je.v2_event_seconds_remaining, je.v3_event_seconds_remaining_placeholder) as game_clock_seconds_remaining,
        coalesce(je.game_clock_time, -- v2's "MM:SS"
            -- TODO: Format v3_event_seconds_remaining_placeholder to "MM:SS" if needed for display
            null
        ) as game_clock_display,
        je.wall_clock_time, -- From v2

        -- Event Classification
        je.event_type as event_type_v2_code, -- integer code from v2
        je.event_action_type as event_action_type_v2_code, -- integer code from v2
        coalesce(je.action_type, je.v3_action_type) as event_name_primary, -- action_type in combined_events is v2.event_type_name or v3.action_type
        coalesce(je.v3_sub_type, je.action_type) as event_name_detail,

        -- Player Information (Limited by int__combined_events providing NULLs for v3 player fields)
        je.player1_id, je.player1_name, je.player1_team_id,
        je.player2_id, je.player2_name, je.player2_team_id,
        je.player3_id, je.player3_name, je.player3_team_id,
        -- If int__combined_events is fixed, player1_id would be: coalesce(je.player1_id, je.v3_player1_id_placeholder)

        -- Score Information
        coalesce(je.v3_home_score, je.v2_parsed_home_score) as home_score,
        coalesce(je.v3_away_score, je.v2_parsed_away_score) as away_score,
        coalesce(
            case when je.v3_home_score is not null and je.v3_away_score is not null then je.v3_home_score - je.v3_away_score else null end,
            je.score_margin -- score_margin from v2 is already numeric in int__combined_events
        ) as score_margin,

        -- Shot Details (Primarily from v3 if matched)
        (je.event_type in ('1', '2') or je.v3_action_type in ('Made Shot', 'Missed Shot')) as is_shot_attempt,
        case
            when je.v3_shot_result = 'Made' then true
            when je.v3_shot_result = 'Missed' then false
            when je.event_type = '1' then true -- v2 made shot
            when je.event_type = '2' then false -- v2 missed shot
            else null
        end as shot_made_flag,
        coalesce(je.v3_points, case when je.event_type = '1' then je.shot_value -- infer from v2 if possible
            else 0 end
        ) as points_scored_on_shot, -- v3.points is 'pointsTotal' from raw v3.
        coalesce(je.v3_shot_value,
             case when lower(je.home_description || je.away_description || je.neutral_description) like '%3pt%' then 3 else 2 end
        ) as shot_value_attempted, -- Basic inference for v2
        je.v3_shot_distance as shot_distance_ft,
        je.v3_shot_x as shot_x_coordinate,
        je.v3_shot_y as shot_y_coordinate,
        je.v3_shot_zone as shot_zone_basic,
        je.v3_sub_type as shot_description_v3, -- e.g., "Jump Shot"

        -- Descriptions
        coalesce(je.home_description, je.away_description, je.neutral_description, je.v3_description) as description_primary,
        je.home_description as description_v2_home,
        je.away_description as description_v2_away,
        je.neutral_description as description_v2_neutral,
        je.v3_description as description_v3_detail,

        -- Other Context
        je.v3_location as location_v3, -- h/v from v3

        greatest(je.created_at, coalesce(je.v3_created_at, je.created_at)) as created_at,
        greatest(je.updated_at, coalesce(je.v3_updated_at, je.updated_at)) as updated_at


    from joined_events je
),

unmatched_v3_events as (
    select
        -- Event Identifiers
        v3_unmatched.game_id,
        v3_unmatched.period,
        v3_unmatched.event_number,
        v3_unmatched.game_id || '_' || v3_unmatched.period || '_' || v3_unmatched.event_number || '_v3_only' as synthesized_event_key,
        'v3_only' as source_synthesis_type,
        v3_unmatched.game_date,

        -- Timing
        v3_unmatched.v3_event_seconds_remaining_placeholder as game_clock_seconds_remaining,
        null::text as game_clock_display, -- TODO: Format v3_event_seconds_remaining_placeholder if needed
        null::text as wall_clock_time,

        -- Event Classification
        null::text as event_type_v2_code,
        null::text as event_action_type_v2_code,
        v3_unmatched.action_type as event_name_primary,
        v3_unmatched.sub_type as event_name_detail,

        -- Player Information (Relies on int__combined_events providing these for v3, currently NULLs)
        v3_unmatched.player1_id, v3_unmatched.player1_name, v3_unmatched.player1_team_id,
        null::integer as player2_id, null::text as player2_name, null::integer as player2_team_id,
        null::integer as player3_id, null::text as player3_name, null::integer as player3_team_id,

        -- Score Information
        v3_unmatched.home_score,
        v3_unmatched.away_score,
        case when v3_unmatched.home_score is not null and v3_unmatched.away_score is not null then v3_unmatched.home_score - v3_unmatched.away_score else null end as score_margin,

        -- Shot Details
        (v3_unmatched.action_type in ('Made Shot', 'Missed Shot')) as is_shot_attempt,
        case
            when v3_unmatched.shot_result = 'Made' then true
            when v3_unmatched.shot_result = 'Missed' then false
            else null
        end as shot_made_flag,
        v3_unmatched.points as points_scored_on_shot,
        v3_unmatched.shot_value as shot_value_attempted,
        v3_unmatched.shot_distance as shot_distance_ft,
        v3_unmatched.shot_x as shot_x_coordinate,
        v3_unmatched.shot_y as shot_y_coordinate,
        v3_unmatched.shot_zone as shot_zone_basic,
        v3_unmatched.sub_type as shot_description_v3,

        -- Descriptions
        v3_unmatched.description as description_primary,
        null::text as description_v2_home,
        null::text as description_v2_away,
        null::text as description_v2_neutral,
        v3_unmatched.description as description_v3_detail,

        -- Other Context
        v3_unmatched.location as location_v3,

        v3_unmatched.created_at,
        v3_unmatched.updated_at

    from v3_events_prepared v3_unmatched
    left join v2_events_parsed v2_check
        on v3_unmatched.game_id = v2_check.game_id
        and v3_unmatched.period = v2_check.period
        and v3_unmatched.event_number = v2_check.event_number
    where v2_check.game_id is null -- Select only v3 events that did not have a v2 match
)

select * from synthesized_from_v2_base
union all
select * from unmatched_v3_events
