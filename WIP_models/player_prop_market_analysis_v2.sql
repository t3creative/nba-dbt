-- WORK IN PROGRESS MODEL
-- depends_on: {{ ref('dim__sportsbooks') }}
-- depends_on: {{ ref('int_betting__player_props_probabilities') }}
{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='market_analysis_key',
        tags=['betting', 'features', 'player_props', 'analysis'],
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['player_id', 'market_id', 'sportsbook_slug']
    )
}}

{% set consensus_slug_var = 'consensus' %}
{% set iterate_slugs = [
  'bet365','betmgm','betrivers','book_33','caesars',
  'consensus','draftkings','fanduel','hardrockbet',
  'partycasino','pinnacle','sugarhouse'
] %}

{% set upstream_relation = ref('int_betting__player_props_probabilities') %}
{% set upstream_cols = [] %}
{% set upstream_col_names = [] %}

with base_data as (

    select * from {{ upstream_relation }}
    {% if is_incremental() %}
        where game_date > (select coalesce(max(game_date), '1900-01-01') from {{ this }})
    {% endif %}

),

unpivoted_data as (

    {% set first_select_done = false %}
    {% for slug in iterate_slugs %}
        {% set over_odds_col        = slug ~ '_over_odds_decimal' %}
        {% set under_odds_col       = slug ~ '_under_odds_decimal' %}
        {% set over_implied_prob_col  = slug ~ '_over_implied_prob' %}
        {% set under_implied_prob_col = slug ~ '_under_implied_prob' %}
        {% set total_implied_prob_col = slug ~ '_total_implied_prob' %}
        {% set over_no_vig_prob_col   = slug ~ '_over_no_vig_prob' %}
        {% set under_no_vig_prob_col  = slug ~ '_under_no_vig_prob' %}
        {% set hold_percentage_col    = slug ~ '_hold_percentage' %}

        {% if slug != consensus_slug_var and over_odds_col in adapter.get_columns_in_relation(upstream_relation) | map(attribute='name') | map('lower') | list %}
            {% if first_select_done %}
select
            {% else %}
            {% set first_select_done = true %}
select
            {% endif %}

        -- common fields for every sportsbook
        player_prop_key,
        game_date,
        player_name,
        player_id,
        player_slug,
        market,
        market_id,
        line,
        consensus_over_odds_decimal,
        consensus_under_odds_decimal,
        consensus_over_implied_prob,
        consensus_under_implied_prob,
        consensus_total_implied_prob,
        consensus_over_no_vig_prob,
        consensus_under_no_vig_prob,
        consensus_hold_percentage,

        -- this sportsbook
        '{{ slug }}' as sportsbook_slug,
        {{ over_odds_col }} as book_over_odds,
        {{ under_odds_col }} as book_under_odds,
        {{ over_implied_prob_col }} as book_over_implied_prob,
        {{ under_implied_prob_col }} as book_under_implied_prob,
        {{ total_implied_prob_col }} as book_total_implied_prob,
        {{ over_no_vig_prob_col }} as book_no_vig_over_prob,
        {{ under_no_vig_prob_col }} as book_no_vig_under_prob,
        {{ hold_percentage_col }} as book_hold_percentage

    from base_data
    where base_data.{{ over_odds_col }} is not null
       or base_data.{{ under_odds_col }} is not null

        {% endif %}
    {% endfor %}

    {% if not first_select_done %}
select
        null::text    as player_prop_key,
        null::date    as game_date,
        null::text    as player_name,
        null::integer as player_id,
        null::text    as player_slug,
        null::text    as market,
        null::integer as market_id,
        null::numeric as line,
        null::numeric as consensus_over_odds_decimal,
        null::numeric as consensus_under_odds_decimal,
        null::numeric as consensus_over_implied_prob,
        null::numeric as consensus_under_implied_prob,
        null::numeric as consensus_total_implied_prob,
        null::numeric as consensus_over_no_vig_prob,
        null::numeric as consensus_under_no_vig_prob,
        null::numeric as consensus_hold_percentage,
        null::text    as sportsbook_slug,
        null::numeric as book_over_odds,
        null::numeric as book_under_odds,
        null::numeric as book_over_implied_prob,
        null::numeric as book_under_implied_prob,
        null::numeric as book_total_implied_prob,
        null::numeric as book_no_vig_over_prob,
        null::numeric as book_no_vig_under_prob,
        null::numeric as book_hold_percentage
    where 1=0
    {% endif %}

),

-- Renaming and ensuring all necessary columns are present for subsequent CTEs
source_props_enhanced as (

    select
        player_prop_key           as prop_key,
        player_prop_key,
        game_date,
        player_name,
        player_id,
        player_slug,
        market,
        market_id,
        line,
        sportsbook_slug,
        book_over_odds,
        book_under_odds,
        book_over_implied_prob,
        book_under_implied_prob,
        book_total_implied_prob,
        book_no_vig_over_prob,
        book_no_vig_under_prob,
        book_hold_percentage,

        case
            when book_no_vig_over_prob > 0
             and book_no_vig_over_prob < 1 then
                case
                    when book_no_vig_over_prob >= 0.5
                    then - (book_no_vig_over_prob / (1 - book_no_vig_over_prob)) * 100
                    else   ((1 - book_no_vig_over_prob) / book_no_vig_over_prob) * 100
                end
            else null
        end as fair_over_odds_american,

        case
            when book_no_vig_under_prob > 0
             and book_no_vig_under_prob < 1 then
                case
                    when book_no_vig_under_prob >= 0.5
                    then - (book_no_vig_under_prob / (1 - book_no_vig_under_prob)) * 100
                    else   ((1 - book_no_vig_under_prob) / book_no_vig_under_prob) * 100
                end
            else null
        end as fair_under_odds_american,

        {{ dbt_utils.generate_surrogate_key(['player_prop_key','sportsbook_slug']) }}
            as market_analysis_key

    from unpivoted_data

),

market_context as (

    select
        market_id,
        market,
        count(*)                 as market_prop_lines_count,
        avg(line)                as avg_market_line,
        percentile_cont(0.5)
          within group (order by line)
                                 as median_market_line,
        stddev(line)             as stddev_market_line,
        min(line)                as min_market_line,
        max(line)                as max_market_line,
        avg(book_total_implied_prob)
                                 as avg_market_total_implied_prob,
        avg(book_hold_percentage)
                                 as avg_market_hold_percentage
    from source_props_enhanced
    group by market_id, market

),

player_context as (

    select
        player_id,
        player_name,
        market_id,
        market,
        count(*)          as player_market_prop_lines_count,
        avg(line)         as avg_player_market_line,
        stddev(line)      as stddev_player_market_line,
        min(line)         as min_player_market_line,
        max(line)         as max_player_market_line
    from source_props_enhanced
    group by player_id, player_name, market_id, market

),

sportsbook_context as (

    select
        sportsbook_slug,
        count(*)               as sportsbook_prop_lines_count,
        avg(book_total_implied_prob)
                               as avg_sportsbook_total_implied_prob,
        avg(book_hold_percentage)
                               as avg_sportsbook_hold_percentage,
        stddev(book_hold_percentage)
                               as stddev_sportsbook_hold_percentage
    from source_props_enhanced
    group by sportsbook_slug

),

final as (

    select
        spe.market_analysis_key,
        spe.player_prop_key    as prop_key,
        spe.player_id,
        spe.player_name,
        spe.market_id,
        spe.market,
        spe.line,
        spe.book_over_odds,
        spe.book_under_odds,
        spe.book_over_implied_prob,
        spe.book_under_implied_prob,
        spe.book_no_vig_over_prob,
        spe.book_no_vig_under_prob,
        spe.fair_over_odds_american,
        spe.fair_under_odds_american,
        spe.book_total_implied_prob,
        spe.book_hold_percentage,
        mc.avg_market_line,
        mc.median_market_line,
        mc.stddev_market_line,
        mc.min_market_line,
        mc.max_market_line,
        mc.avg_market_total_implied_prob,
        mc.avg_market_hold_percentage,
        pc.avg_player_market_line,
        pc.stddev_player_market_line,
        pc.min_player_market_line,
        pc.max_player_market_line,
        spe.sportsbook_slug,
        sc.avg_sportsbook_total_implied_prob,
        sc.avg_sportsbook_hold_percentage,
        sc.stddev_sportsbook_hold_percentage,
        spe.line - mc.avg_market_line
                                 as line_vs_market_avg,
        spe.line - pc.avg_player_market_line
                                 as line_vs_player_avg,
        spe.book_hold_percentage - sc.avg_sportsbook_hold_percentage
                                 as hold_vs_book_avg,
        spe.book_hold_percentage - mc.avg_market_hold_percentage
                                 as hold_vs_market_avg,
        spe.game_date,
        current_timestamp     as analysis_timestamp

    from source_props_enhanced spe
    left join market_context     mc on spe.market_id     = mc.market_id
    left join player_context     pc on spe.player_id     = pc.player_id
                                   and spe.market_id    = pc.market_id
    left join sportsbook_context sc on spe.sportsbook_slug = sc.sportsbook_slug

)

select * from final
