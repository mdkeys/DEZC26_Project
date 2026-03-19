{{
    config(
        materialized='table',
        cluster_by=['complaint_type', 'borough']
    )
}}

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

aggregated as (
    select
        complaint_type,
        descriptor,
        borough,

        count(unique_key)                                    as total_complaints,
        countif(is_resolved)                                 as resolved_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                             as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                             as avg_resolution_days,
        countif(not is_resolved)                             as open_complaints,
        countif(resolved_within_30_days)                     as resolved_within_30_days

    from enriched
    group by
        complaint_type,
        descriptor,
        borough
)

select * from aggregated
order by total_complaints desc
