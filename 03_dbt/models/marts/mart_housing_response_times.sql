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

/*
    Analysis 2: Housing Maintenance Response Times by Borough
    
    Shows average resolution time for housing maintenance complaints by borough,
    complaint type, and year. Surfaces disparities in agency response across boroughs.
    
    Key insight: ~28% of all housing complaints are closed without a confirmed
    inspection (Violation Watch, 2025). Resolution time disparities by borough
    point to resource allocation gaps in city enforcement.
*/

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

response_times as (
    select
        borough,
        complaint_type,
        year,

        count(unique_key)                                       as total_complaints,
        countif(is_resolved)                                    as resolved_complaints,
        countif(not is_resolved)                                as open_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                                as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                                as avg_resolution_days,
        round(
            approx_quantiles(
                case when is_resolved then resolution_days end, 2
            )[offset(1)], 1
        )                                                       as median_resolution_days,
        countif(resolved_within_30_days)                        as resolved_within_30_days,
        round(
            countif(resolved_within_30_days) / nullif(countif(is_resolved), 0) * 100, 2
        )                                                       as pct_resolved_within_30_days

    from enriched
    where complaint_type in {{ housing_complaint_types() }}
    group by
        borough,
        complaint_type,
        year
)

select * from response_times
