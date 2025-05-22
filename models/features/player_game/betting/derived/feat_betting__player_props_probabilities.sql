-- depends_on: {{ ref('dim__sportsbooks') }}
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_prop_key',
        on_schema_change='sync_all_columns',
        tags=['betting', 'features', 'player_props', 'probabilities']
    )
}}

{%- set consensus_slug_var = 'consensus' -%} 
{%- set placeholder_slug = 'placeholder_slug_for_parsing' -%}

{# Fetch distinct sportsbook slugs again to align with columns from the pivoted model #}
{%- set distinct_sportsbook_slugs = [placeholder_slug] -%} {# Default for parsing #}
{%- if execute -%}
    {%- set sportsbook_slugs_query %}
        select distinct sportsbook_slug
        from {{ ref('dim__sportsbooks') }}
        order by 1
    {%- endset %}
    {%- set results = run_query(sportsbook_slugs_query) -%}
    {%- if results and results.columns[0].values() | length > 0 -%}
        {%- set distinct_sportsbook_slugs = results.columns[0].values() | list -%}
    {%- endif -%} 
{%- endif -%}

with source_pivoted_props as (
    select * from {{ ref('int_betting__player_props_pivoted') }}
    
    {% if is_incremental() %}
        -- Assuming game_date is available for incremental logic
        where game_date > (select coalesce(max(game_date), '1900-01-01') from {{ this }})
    {% endif %}
),

final_with_calculations as (
    select
        -- Core descriptive columns (as defined in int_betting__player_props_pivoted)
        spp.game_date,
        spp.player_name,
        spp.market,
        spp.line,

        -- Consensus Odds (from int_betting__player_props_pivoted)
        spp.over_odds_decimal as consensus_over_odds_decimal,
        spp.under_odds_decimal as consensus_under_odds_decimal,

        -- Consensus Calculations (newly added here)
        round(({{ decimal_to_probability('spp.over_odds_decimal') }})::numeric, 4) as consensus_over_implied_prob,
        round(({{ decimal_to_probability('spp.under_odds_decimal') }})::numeric, 4) as consensus_under_implied_prob,
        round((({{ decimal_to_probability('spp.over_odds_decimal') }} + {{ decimal_to_probability('spp.under_odds_decimal') }}))::numeric, 4) as consensus_total_implied_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.over_odds_decimal'), decimal_to_probability('spp.under_odds_decimal')) }})::numeric, 4) as consensus_over_no_vig_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.under_odds_decimal'), decimal_to_probability('spp.over_odds_decimal')) }})::numeric, 4) as consensus_under_no_vig_prob,
        case 
            when ({{ decimal_to_probability('spp.over_odds_decimal') }} + {{ decimal_to_probability('spp.under_odds_decimal') }}) is not null 
            then round((({{ decimal_to_probability('spp.over_odds_decimal') }} + {{ decimal_to_probability('spp.under_odds_decimal') }}) - 1.0)::numeric, 4) 
            else null 
        end as consensus_hold_percentage,

        -- Key identifier columns
        spp.player_prop_key,
        spp.player_id,
        spp.player_slug,
        spp.market_id,

        {# Individual Sportsbook Odds Columns (from int_betting__player_props_pivoted) #}
        {%- for slug in distinct_sportsbook_slugs %}
            {%- if slug != consensus_slug_var and slug != placeholder_slug %}
        spp.{{ 'over_odds_' ~ slug ~ '_decimal' }} as {{ slug ~ '_over_odds_decimal' }},
        spp.{{ 'under_odds_' ~ slug ~ '_decimal' }} as {{ slug ~ '_under_odds_decimal' }},
            {%- elif slug == placeholder_slug and placeholder_slug != consensus_slug_var and distinct_sportsbook_slugs | length == 1 %} {# if placeholder is the only slug and not consensus #}
        spp.{{ 'over_odds_' ~ placeholder_slug ~ '_decimal' }} as {{ placeholder_slug ~ '_over_odds_decimal' }},
        spp.{{ 'under_odds_' ~ placeholder_slug ~ '_decimal' }} as {{ placeholder_slug ~ '_under_odds_decimal' }},
            {%- endif %}
        {%- endfor %}

        {# Individual Sportsbook Calculated Columns (newly added here) #}
        {%- for slug in distinct_sportsbook_slugs %}
            {%- if slug != consensus_slug_var and slug != placeholder_slug %}
        round(({{ decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal') }})::numeric, 4) as {{ slug }}_over_implied_prob,
        round(({{ decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal') }})::numeric, 4) as {{ slug }}_under_implied_prob,
        round((({{ decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal') }}))::numeric, 4) as {{ slug }}_total_implied_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal'), decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal')) }})::numeric, 4) as {{ slug }}_over_no_vig_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal'), decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal')) }})::numeric, 4) as {{ slug }}_under_no_vig_prob,
        case 
            when ({{ decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal') }}) is not null
            then round((({{ decimal_to_probability('spp.over_odds_' ~ slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ slug ~ '_decimal') }}) - 1.0)::numeric, 4)
            else null
          end as {{ slug }}_hold_percentage{%- if not loop.last or (placeholder_slug in distinct_sportsbook_slugs and placeholder_slug != consensus_slug_var and distinct_sportsbook_slugs | length == 1) %},{% endif %}
            {%- elif slug == placeholder_slug and placeholder_slug != consensus_slug_var and distinct_sportsbook_slugs | length == 1 %} {# if placeholder is the only slug and not consensus #}
        round(({{ decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal') }})::numeric, 4) as {{ placeholder_slug }}_over_implied_prob,
        round(({{ decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal') }})::numeric, 4) as {{ placeholder_slug }}_under_implied_prob,
        round((({{ decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal') }}))::numeric, 4) as {{ placeholder_slug }}_total_implied_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal'), decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal')) }})::numeric, 4) as {{ placeholder_slug }}_over_no_vig_prob,
        round(({{ no_vig_probability(decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal'), decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal')) }})::numeric, 4) as {{ placeholder_slug }}_under_no_vig_prob,
        case 
            when ({{ decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal') }}) is not null
            then round((({{ decimal_to_probability('spp.over_odds_' ~ placeholder_slug ~ '_decimal') }} + {{ decimal_to_probability('spp.under_odds_' ~ placeholder_slug ~ '_decimal') }}) - 1.0)::numeric, 4)
            else null
          end as {{ placeholder_slug }}_hold_percentage
            {%- endif %}
        {%- endfor %}

    from source_pivoted_props spp
)

select * from final_with_calculations