## Immigration rate per US state

immigration_rate_us_states ="""

with A as (

select 
origin_country_code,
year,
month,
state_code,
count ,
sum(count) over () as total_immigrants,
sum(count) over (partition by state_code) as total_immigrants_state,
sum(count) over (partition by state_code,origin_country_code  order by  origin_country_code desc ) as totals_state_country

from ds_facts 
),

S as (
    select 
    state_name,
    id
    from 
    ds_state
),

C as (
    select
    code,
    country
    from ds_countries

)

Select 
A.year,
A.month,
S.state_name,
C.country,
A.total_immigrants,
A.total_immigrants_state,
sum(count) as immigrants_by_country,
round((sum(count) /A.total_immigrants_state),4)*100 as percent_total

FROM A
join S
on A.state_code= S.id
join C 
on A.origin_country_code = C.code

group by 1,2,3,4,5,6
order by (total_immigrants_state,immigrants_by_country) desc


"""

