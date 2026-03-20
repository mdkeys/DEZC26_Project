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
        cluster_by=['borough', 'month']
    )
}}

/*
    Analysis 1: Heat & Hot Water Failure Patterns by Borough and Season
    
    Shows monthly volume of HEAT/HOT WATER complaints by borough from 2020 to present.
    Enables trend analysis across heat seasons (October-May) and year-over-year comparison.
    
    Key insight: Heat complaints have more than doubled since 2016 and are concentrated
    in communities of color (NYC Comptroller, Turn Up the Heat 2025).
*/

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

heat_complaints as (
    select
        borough,
        year,
        month,

        -- heat season flag: October (10) through May (5)
        case
            when month between 10 and 12 then 'heat season'
            when month between 1  and 5  then 'heat season'
            else 'non-heat season'
        end as season,

        count(unique_key)                                       as total_complaints,
        countif(is_resolved)                                    as resolved_complaints,
        countif(not is_resolved)                                as open_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                                as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                                as avg_resolution_days,
        countif(resolved_within_30_days)                        as resolved_within_30_days,
        round(
            countif(resolved_within_30_days) / nullif(countif(is_resolved), 0) * 100, 2
        )                                                       as pct_resolved_within_30_days

    from enriched
    where complaint_type = 'HEAT/HOT WATER'
    group by
        borough,
        year,
        month,
        season
)

select * from heat_complaints
