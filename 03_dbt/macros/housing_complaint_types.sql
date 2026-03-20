{# 
    housing_complaint_types()
    Returns a list of complaint_type values considered housing maintenance complaints.
    Used across all mart models to ensure consistent filtering.
#}

{% macro housing_complaint_types() %}
    (
        'HEAT/HOT WATER',
        'PLUMBING',
        'WATER LEAK',
        'FLOORING/STAIRS',
        'ELECTRIC',
        'APPLIANCE',
        'PAINT/PLASTER',
        'DOOR/WINDOW',
        'UNSANITARY CONDITION',
        'GENERAL',
        'Elevator'
    )
{% endmacro %}
