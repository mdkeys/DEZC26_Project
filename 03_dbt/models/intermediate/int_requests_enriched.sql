{{
    config(
        materialized='view'
    )
}}

with staged as (
    select * from {{ ref('stg_311_requests') }}
),

enriched as (
    select
        -- passthrough all staging columns
        unique_key,
        created_date,
        closed_date,
        due_date,
        complaint_type,
        descriptor,
        descriptor_2,
        resolution_description,
        location_type,
        status,
        agency,
        borough,
        incident_zip,
        community_board,
        council_district,
        police_precinct,
        city,
        latitude,
        longitude,
        year,
        month,

        -- derived: complaint category
        case
            when complaint_type = 'HEAT/HOT WATER'  then 'HEAT_HOTWATER'
            when complaint_type in ('PLUMBING', 'WATER LEAK') then 'PLUMBING_WATERLEAK'
            when complaint_type in ('ELECTRIC', 'APPLIANCE') then 'ELECTRIC_APPLIANCE'
            when complaint_type = 'Elevator' then 'ELEVATOR'
            when complaint_type in (
                'DOOR/WINDOW', 'FLOORING/STAIRS', 'GENERAL',
                'PAINT/PLASTER', 'UNSANITARY CONDITION'
            ) then 'OTHER'
            else 'OTHER'
        end as complaint_category,

        -- derived: resolution time in days
        case
            when closed_date is not null and closed_date >= created_date
            then date_diff(date(closed_date), date(created_date), day)
            else null
        end as resolution_days,

        -- derived: whether the request has been resolved
        case
            when lower(status) = 'closed' then true
            else false
        end as is_resolved,

        -- derived: whether the request was resolved within 30 days
        case
            when closed_date is not null
                and closed_date >= created_date
                and date_diff(date(closed_date), date(created_date), day) <= 30
            then true
            else false
        end as resolved_within_30_days,

        -- derived: time of day bucket for created_date
        case
            when extract(hour from created_date) between 6  and 11 then 'morning'
            when extract(hour from created_date) between 12 and 17 then 'afternoon'
            when extract(hour from created_date) between 18 and 21 then 'evening'
            else 'night'
        end as time_of_day,

        -- derived: day of week
        format_date('%A', date(created_date)) as day_of_week

    from staged
)

select * from enriched