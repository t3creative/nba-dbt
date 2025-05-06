{% macro export_all_neo4j_models(output_path) %}
    -- Define the order of export to ensure relationships are created after nodes
    {% set node_models = [
        'int_neo4j_export__nodes_players',
        'int_neo4j_export__nodes_teams',
        'int_neo4j_export__nodes_games',
        'int_neo4j_export__nodes_venues',
        'int_neo4j_export__nodes_seasons',
        'int_neo4j_export__nodes_awards',
        'int_neo4j_export__nodes_injuries',
        'int_neo4j_export__nodes_draft',
        'int_neo4j_export__nodes_colleges',
        'int_neo4j_export__nodes_countries'
    ] %}
    
    {% set relationship_models = [
        'int_neo4j_export__rels_plays_at',
        'int_neo4j_export__rels_part_of',
        'int_neo4j_export__rels_team_game',
        'int_neo4j_export__rels_played_in',
        'int_neo4j_export__rels_played_for',
        'int_neo4j_export__rels_received',
        'int_neo4j_export__rels_suffered',
        'int_neo4j_export__rels_teammates',
        'int_neo4j_export__rels_drafted_in',
        'int_neo4j_export__rels_attended',
        'int_neo4j_export__rels_from_country'
    ] %}
    
    -- Export all node models first
    {% for model in node_models %}
        {{ log("Exporting node model: " ~ model, info=True) }}
        {% do run_macro("export_table_to_csv", model, output_path ~ "/nodes") %}
    {% endfor %}
    
    -- Then export all relationship models
    {% for model in relationship_models %}
        {{ log("Exporting relationship model: " ~ model, info=True) }}
        {% do run_macro("export_table_to_csv", model, output_path ~ "/relationships") %}
    {% endfor %}
    
    {{ return("Successfully exported all Neo4j models to " ~ output_path) }}
{% endmacro %} 