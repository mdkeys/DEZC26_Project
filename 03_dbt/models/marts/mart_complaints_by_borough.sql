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

aggregated as (
    select
        borough,
        year,
        month,
        complaint_type,

        count(unique_key)                                    as total_complaints,
        countif(is_resolved)                                 as resolved_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                             as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                             as avg_resolution_days

    from enriched
    group by
        borough,
        year,
        month,
        complaint_type
)

select * from aggregated
