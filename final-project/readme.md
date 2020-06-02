## Capstone-Project
### Scope
1. I am using here US Immigration data to provide single source of truth to all users at US Immigrations
2. From the Data we will be able to do analysis on Immigration rate of different countries and cities
3. Which states/cities airport are used mostly

### Technologies used
1. Spark sql to read data and do ETL and store this Data into S3
2. From S3 we can use Airflow to load data into Redshift 
3. Choice of redshift is made because of managed service from AWS


### Architecture
local/S3 >> spark >> S3 
Airflow >> Load Data to S3 with cronjob schedule

### End Result
Single source of truth
Provide some usefull analysis on Immigration data


## Schema 
|-- cicid: double (nullable = true)  : unique identifier
 |-- i94yr: double (nullable = true) : year
 |-- i94mon: double (nullable = true):  month
 |-- i94cit: double (nullable = true): city code
 |-- i94res: double (nullable = true): city code
 |-- i94port: string (nullable = true):  codes for processing (where immigrant came)
 |-- arrdate: double (nullable = true): arrival date 
 |-- i94mode: double (nullable = true) : arival mode
 |-- i94addr: string (nullable = true) :
 |-- depdate: double (nullable = true) : Departure Date from the USA
 |-- i94bir: double (nullable = true):  Age of Respondent in Years
 |-- i94visa: double (nullable = true): Visa codes collapsed into three categories
 |-- count: double (nullable = true): Used for summary statistics
 |-- dtadfile: string (nullable = true):Character Date Field - Date added to I-94 Files 
 |-- visapost: string (nullable = true): 
 |-- occup: string (nullable = true)
 |-- entdepa: string (nullable = true)
 |-- entdepd: string (nullable = true)
 |-- entdepu: string (nullable = true)
 |-- matflag: string (nullable = true)
 |-- biryear: double (nullable = true)
 |-- dtaddto: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- insnum: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- admnum: double (nullable = true)
 |-- fltno: string (nullable = true)
 |-- visatype: string (nullable = true)

 /* VISAPOST - Department of State where where Visa was issued - CIC does not use */


/* OCCUP - Occupation that will be performed in U.S. - CIC does not use */


/* ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use */


/* ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use */


/* ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use */


/* MATFLAG - Match flag - Match of arrival and departure records */


/* BIRYEAR - 4 digit year of birth */


/* DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use */


/* GENDER - Non-immigrant sex */


/* INSNUM - INS number */


/* AIRLINE - Airline used to arrive in U.S. */


/* ADMNUM - Admission Number */


/* FLTNO - Flight number of Airline used to arrive in U.S. */


/* VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S. */

