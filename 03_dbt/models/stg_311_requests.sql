{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nyc_311_raw', 'external_311_requests') }}
),

renamed as (
    select
        -- identifiers
        unique_key,

        -- timestamps
        cast(created_date as timestamp)  as created_date,
        cast(closed_date  as timestamp)  as closed_date,
        cast(due_date     as timestamp)  as due_date,

        -- complaint details
        complaint_type,
        descriptor,
        location_type,
        lower(status)                    as status,

        -- agency
        agency,

        -- geography
        upper(trim(borough))             as borough,
        incident_zip,
        community_board,
        cast(council_district as int64)  as council_district,
        cast(police_precinct  as int64)  as police_precinct,
        city,
        cast(latitude  as float64)       as latitude,
        cast(longitude as float64)       as longitude,

        -- partition columns
        cast(year  as int64)             as year,
        cast(month as int64)             as month

    from source
    where
        -- exclude rows with no unique key or created date
        unique_key   is not null
        and created_date is not null
        -- exclude invalid borough values
        and upper(trim(borough)) in (
            'MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND'
        )
)

select * from renamed
