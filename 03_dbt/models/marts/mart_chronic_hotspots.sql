{{
    config(
        materialized='table',
        cluster_by=['community_board', 'complaint_type']
    )
}}

/*
    Analysis 3: Chronic Housing Complaint Hotspots by Community District
    
    Identifies community districts with the highest concentration of housing
    maintenance complaints across all years. A district is considered a "chronic
    hotspot" if it appears in the top quartile of complaint volume for 3+ years.
    
    Key insight: Of 901 buildings on the persistent heat offender list, 20% had
    no HPD enforcement action for 7 years (NYC Comptroller, Turn Up the Heat 2025).
    Community-district-level patterns reveal systemic landlord negligence.
*/

with enriched as (
    select * from {{ ref('int_requests_enriched') }}
),

-- aggregate complaints by community board, complaint type, and year
annual_complaints as (
    select
        community_board,
        borough,
        complaint_type,
        year,

        count(unique_key)                                       as total_complaints,
        countif(is_resolved)                                    as resolved_complaints,
        round(countif(is_resolved) / count(unique_key) * 100, 2)
                                                                as pct_resolved,
        round(avg(case when is_resolved then resolution_days end), 1)
                                                                as avg_resolution_days

    from enriched
    where
        complaint_type in {{ housing_complaint_types() }}
        and community_board is not null
    group by
        community_board,
        borough,
        complaint_type,
        year
),

-- count how many years each community board appears as a high-complaint district
years_as_hotspot as (
    select
        community_board,
        borough,
        complaint_type,
        count(distinct year)                                    as years_with_data,
        sum(total_complaints)                                   as total_complaints_all_years,
        round(avg(total_complaints), 1)                         as avg_annual_complaints,
        round(avg(pct_resolved), 2)                             as avg_pct_resolved,
        round(avg(avg_resolution_days), 1)                      as avg_resolution_days,
        min(year)                                               as first_year,
        max(year)                                               as last_year

    from annual_complaints
    group by
        community_board,
        borough,
        complaint_type
),

-- flag chronic hotspots: districts appearing in top 25% of complaint volume for 3+ years
hotspots as (
    select
        *,
        case
            when years_with_data >= 3
                and avg_annual_complaints >= percentile_cont(avg_annual_complaints, 0.75)
                    over (partition by complaint_type)
            then true
            else false
        end                                                     as is_chronic_hotspot

    from years_as_hotspot
)

select * from hotspots
