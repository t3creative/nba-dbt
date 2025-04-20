{% macro distance_between_arenas(lat1, lon1, lat2, lon2) %}
    /*
    Calculates the Haversine distance between two points on Earth using latitude and longitude.
    
    Args:
        lat1 (float): Latitude of the first point
        lon1 (float): Longitude of the first point
        lat2 (float): Latitude of the second point
        lon2 (float): Longitude of the second point
        
    Returns:
        Distance in miles between the two points
        
    Example:
        {{ distance_between_arenas('arena1.lat', 'arena1.long', 'arena2.lat', 'arena2.long') }} as travel_distance
    */
    
    (
        3959 * acos(
            cos(radians({{ lat1 }})) * 
            cos(radians({{ lat2 }})) * 
            cos(radians({{ lon2 }}) - radians({{ lon1 }})) + 
            sin(radians({{ lat1 }})) * 
            sin(radians({{ lat2 }}))
        )
    )
{% endmacro %} 