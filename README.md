**Star-Schema-Project using PySpark**
**The Bike Sharing Programme**

**Introduction**
>This project revolves around building a Star schema using Biking data. Using this star Schema, organizations can efficiently perform business queries. Customers can use the bike sharing programme to lease  a bike from any station operated by the company, can ride it within the allowed premisis for a predetermined period of time, and then park it back to either same or different station. Here only one databricks pyspark notebook is used to load the data, perform transformations, create fact and dimension tables and answer business questions mentioned down below the page.  

**Data used**

| Data | Description |
| --- | --- |
| payments.csv | This file includes trip payment data: [payment_id, date, amount, account_num] |
| riders.csv | This file includes riders profile data: [rider_id, address, first_name, last_name, DOB]  |
| trips.csv | This file includes data around the trip: [trip_id, rideable_type, started_at, ended_at, start_station_id, end_station_id, member_id] |
| stations.csv | This file includes data around the station: [station_id, name, longitude, latitude] |

**Data Modeling for Star Schema **
** 1. Conceptual Model **
https://app.dbdesigner.net/designer/schema/0-star-schema-p2


**Data loading**
>Data is uploaded and schema is defined in the first cell of the python file uploaded above. 
