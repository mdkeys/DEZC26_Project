{{
    config(
        materialized='table',
        partition_by={
            'field': 'year',
            'data_type': 'int64',
            'range': {
                'start': 2020,
                'end': 2030,
                'interval': 1
            }
        },
        cluster_by=['borough', 'complaint_type']
    )
}}

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

geo as (
    select
        unique_key,
        complaint_type,
        descriptor,
        borough,
        incident_zip,
        community_board,
        council_district,
        police_precinct,
        latitude,
        longitude,
        date(created_date)  as created_date,
        year,
        month,
        is_resolved,
        resolution_days

    from enriched
    where
        -- only include rows with valid coordinates
        latitude  is not null
        and longitude is not null
        -- basic NYC bounding box sanity check
        and latitude  between 40.4 and 40.95
        and longitude between -74.3 and -73.65
)

select * from geo
