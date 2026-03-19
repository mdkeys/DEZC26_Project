{{
    config(
        materialized='table',
        cluster_by=['agency', 'complaint_type']
    )
}}

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

aggregated as (
    select
        agency,
        complaint_type,
        borough,
        year,

        count(unique_key)                                    as total_complaints,
        countif(is_resolved)                                 as resolved_complaints,
        countif(not is_resolved)                             as open_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                             as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                             as avg_resolution_days,
        min(case when is_resolved then resolution_days end)  as min_resolution_days,
        max(case when is_resolved then resolution_days end)  as max_resolution_days,
        countif(resolved_within_30_days)                     as resolved_within_30_days,
        round(countif(resolved_within_30_days) / nullif(countif(is_resolved), 0) * 100, 2)
                                                             as pct_resolved_within_30_days

    from enriched
    group by
        agency,
        complaint_type,
        borough,
        year
)

select * from aggregated
order by total_complaints desc
