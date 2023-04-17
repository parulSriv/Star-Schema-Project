**Documentation for Star Schema Project- Pyspark | Databricks**
**The Bike Sharing Programme**

**Introduction**
>This project revolves around building a Star schema using Biking data. Using this star Schema, organizations can efficiently perform business queries. Customers can use the bike sharing programme to lease a bike from any station operated by the company, can ride it within the allowed premises for a predetermined period, and then park it back at either the same or a different station. Here only one databricks pyspark notebook is used to load the data, perform transformations, create fact and dimension tables and answer business questions mentioned below the page.  

**Data used**

| Data | Description |
| --- | --- |
| payments.csv | This file includes trip payment data: [payment_id, date, amount, account_num] |
| riders.csv | This file includes riders profile data: [rider_id, address, first_name, last_name, DOB]  |
| trips.csv | This file includes data around the trip: [trip_id, rideable_type, started_at, ended_at, start_station_id, end_station_id, member_id] |
| stations.csv | This file includes data around the station: [station_id, name, longitude, latitude] |


**Conceptual Model plus Logical model for Star Schema**
>It is an organized business overview of the data needed to support business processes, document business events, and monitor associated performance indicators provided by the conceptual data model. The following link also includes a logical model explaining the relationship within the data a bit in detail:
https://app.dbdesigner.net/designer/schema/0-star-schema-p2


**Data loading**
>Data is uploaded and schema is defined in the first cell of the python file uploaded above. 

**Data transformation**
>As per the business requirements, few transformations needs to be done such as:

>*1. Trip duration calculation using trips.csv*
>
>*2. Age of the rider from the birthday column using riders.csv*
>
>*3. Split month and year from date  using payment.csv*

**Creation of FACT table**
>To create a fact table, all above mentioned raw data needs to be converted and cleaned to create respective dimension tables. Reference can be taken from https://app.dbdesigner.net/designer/schema/0-star-schema-p2. 
>Using spark.sql: complex join operation, combine all the dimension tables to create one FACT table using a foreign key. The code for the same is provided in the notebook attached above. 


**Business Requirements:**

Q1) Analyse how much time is spent per ride:

a) Based on date and time factors such as day of week and time of day
b) Based on which station is the starting station and ending station
c) Based on age of the rider at time of the ride
d) Based on whether the rider is a member or a casual rider

Q2) Analyse how much money is spent:

a) Per month, quarter, year
b) Per member, based on the age of the rider at account start


Q3) **EXTRA CREDIT** - Analyse how much money is spent per member:

a) Based on how many rides the rider averages per month
b) Based on how many minutes the rider spends on a bike per month

**To support findings, visualizations/plot has been drawn using python library matplotlib.**
