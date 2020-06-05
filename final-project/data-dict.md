### DATA DICTIONARY

#### Transformed layer from Data Lake
**facts_immigration**
  - cicid: surrogate key
  - year: 4 digits integer
  - month: 2 digits integer
  - entry_number: unique entry number allocated to an immigrant  
  - origin_country_code: code of the origin coutry
  - arrival_port_code: 3 digits arrival port code
  - sas_arrival_date: arrival date
  - sas_departure_date: departure date 
  - state_code: us state code
  - count: immigrant count(always 1 for each row)
  - departue_status: departure flag - departed, lost i-94 or is deceased 
  - departure_status_update: update flag - either apprehended, overstayed, adjusted to perm residence

**dim_personal**
  - id: unique entry number allocated to an immigrant  
  - gender: gender
  - birth_year: birth year 
  - entry_mode: entry mod code (1,2,3)
  - visa_category: visa category code
  - origin_country: code of the origin coutry


**dim_us_demographics**
  - city: city
  - state: state
  - median_age: median_age
  - males: males
  - females: females
  - total_population: total_population
  - veterans: veterans
  - foreign-born: foreign-born
  - average_household: average_household
  - state_code: state_code
  - race: race
  - house_holds: house_holds


**dim_date**
 - Date: date 
 - Date_key: date in int format
 - Day: day name
 - Week: week number 
 - Quarter: quarter number
 - Year: 4 digit integer  
 - Year_half: 1 digit integer

**country_mapping**
 - code: Code of the origin coutry
 - country: country name

**mode_of_entry_mapping**
 - id: mode of entry code
 - entry_mode: type of entry mode

**port_entry_mapping**
 - id: port code
 - entry_port: name of entry port
 - city: city of entry port
 - state: US state code
 - state_1: In case field has 2 delimeters, can be ignored

**state_of_address_mapping**
 - id: state id
 - state_name: state_name

**visa_cat_mapping**
 - id: visa category id
 - visa_type: visa type business, study , travel


#### Step4.1:  Analysis layer from Data Lake

**immigration_rate_us_states**
 - year:  4 digits integer
 - month: 2 digits integer 
 - state_name: name of us state
 - country: origin country name
 - total_immigrants: total immigrants in this month and year
 - total_immigrants_state: total immigrants of us state
 - immigrants_by_country: immigrant by country
 - percent_total: percent total for each state comming from different origin countries