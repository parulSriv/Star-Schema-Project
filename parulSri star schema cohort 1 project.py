# Databricks notebook source
# DBTITLE 1,Defined Schema for Riders, payments, stations and trips dataset
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType, TimestampType, DateType, BooleanType
from pyspark.sql import column as col
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date


# # ############################################
# # #Riders datasets
riders_00 = "/FileStore/tables/riders.csv"
results_schema = StructType(fields=[StructField("rider_id",IntegerType(),False),
                                    StructField("first_name",StringType(),True),
                                    StructField("last_name", StringType(), True),
                                    StructField("address",StringType(),True),
                                    StructField("birthday_date", StringType(), True),
                                    StructField("account_start_date", DateType(), True),
                                    StructField("account_end_date", DateType(), True),
                                    StructField("is_member", BooleanType(), True)])

df_riders= spark.read.schema(results_schema).csv("/FileStore/tables/riders.csv")
df_riders.display()
#df_riders.printSchema()


# # ############################################
# # #Payments dataset
payments_data = "/FileStore/tables/payments.csv"
results_schema2 = StructType(fields=[StructField("payment_id",IntegerType(),False),
                                    StructField("date",DateType(),True),
                                    StructField("amount",DecimalType(),True),
                                    StructField("rider_id", IntegerType(), True)])
df_payment= spark.read.schema(results_schema2).csv("/FileStore/tables/payments.csv")


df_payment.display()
# # df_payment.printSchema()




# # ############################################
# #stations dataset
stations_data = "/FileStore/tables/stations.csv"
results_schema_Station= StructType(fields=[StructField("station_id",IntegerType(),False),
                                    StructField("name",StringType(),True),
                                    StructField("longitude",FloatType(),True),
                                    StructField("latitude", FloatType(), True)])
df_stations= spark.read.schema(results_schema_Station).csv("/FileStore/tables/stations.csv")

# # # df_stations.display()
# # df_stations.printSchema()






############################################
#Trips Dataset
trips_data = "/FileStore/tables/trips.csv"
results_schema3 = StructType(fields=[StructField("trip_id",StringType(),False),
                                    StructField("rideable_type",StringType(),True),                                                                                       StructField("started_at",StringType(),True), # not responding for TimestampType and datetype.
                                    StructField("ended_at",StringType(),True),
                                    StructField("start_station_id",IntegerType(),True),
                                    StructField("end_station_id",IntegerType(),True),
                                    StructField("member_id", IntegerType(), True)])
df_trips= spark.read.schema(results_schema3).csv("/FileStore/tables/trips.csv")

# Using select
df_trips= df_trips.withColumn("started_at",df_trips.started_at.cast(TimestampType()))
# # df_trips.display()
# df_trips.printSchema()


# COMMAND ----------

# DBTITLE 1,Upload it into gold/silver folder
# df_trips.write.mode("overwrite").format("parquet").saveAsTable("trips_table_parquet")
# df_payment.write.mode("overwrite").format("parquet").saveAsTable("df_payment_parquet")
# df_riders.write.mode("overwrite").format("parquet").saveAsTable("df_riders_parquet")
# df_stations.write.mode("overwrite").format("parquet").saveAsTable("df_stations_parquet")

#//Using overwrite
# df_trips.write.mode("overwrite").csv("/FileStore/tables/silver/trips.csv").

# COMMAND ----------

# DBTITLE 1,Duration Calculation - Trips CSV
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import to_date
from dateutil.parser import parse
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
        
raw_data= spark.read.schema(results_schema3).csv("/FileStore/tables/trips.csv")
data= raw_data.withColumn('started_at', to_timestamp(col("started_at"),"dd/MM/yyyy HH:mm").alias("started_at")) \
             .withColumn('ended_at', to_timestamp(col("ended_at"),"dd/MM/yyyy HH:mm").alias("ended_at"))


#Calculating time duration of the journey, finding week_day_number and week_day_name
df_trips= data.withColumn(
    "Trip_Duration_Minutes", 
    (F.col("ended_at").cast("long") - F.col("started_at").cast("long"))/60) \
          .withColumn("week_day_number", date_format(col("started_at"), "u")) \
          .withColumn("week_day_name", date_format(col("started_at"), "EEEE")) \
          .withColumn("Trip_start_date_month", month(data.started_at).cast('int'))

df_trips= df_trips.withColumn("week_day_number", df_trips.week_day_number.cast('integer'))

df_trips.display()

# COMMAND ----------

# DBTITLE 1,Age calculation riders .csv
from pyspark.sql import functions as F
df_riders = df_riders.withColumn('age', (F.months_between(current_date(), F.col('birthday_date')) / 12).cast('int'))
# df_riders.display()
# df_riders.printSchema()

#separate month and year from account_start_date
df_riders = df_riders.withColumn('year_account_start_date', year(df_riders.account_start_date).cast('int'))
df_riders.display()

# #Visualize Age Distribution across the data
import matplotlib.pyplot as plt
df_riders_toPandas= df_riders.toPandas()

plt.hist(df_riders_toPandas.age, bins=10, edgecolor='black')
# Set the title and labels
plt.title('Age Distribution')
plt.xlabel('Age')
plt.ylabel('Count')

# Show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1,Split month and year from date- payment CSV
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date
df_payment.printSchema()
df_payment = df_payment.withColumn('year_date',year(df_payment.date)) \
                       .withColumn('month_date',month(df_payment.date)) \
                       .withColumn('quarter_date',quarter(df_payment.date))

df_payment.display()

# COMMAND ----------

# DBTITLE 1,Creating Fact Table
#Creating Dimension Table

# df_trips.createOrReplaceTempView("df_trips_dim_table")
# df_riders.createOrReplaceTempView("df_riders_dim_table")
# df_payment.createOrReplaceTempView("df_payment_dim_table")
# df_stations.createOrReplaceTempView("df_stations_dim_table")


## Creating Fact Table

fact_table= spark.sql("SELECT df_trips_dim_table.trip_id, df_trips_dim_table.start_station_id, df_trips_dim_table.end_station_id, df_trips_dim_table.member_id, df_riders_dim_table.rider_id, df_trips_dim_table.Trip_Duration_Minutes ,  df_riders_dim_table.age, df_payment_dim_table.payment_id, df_payment_dim_table.amount  FROM  df_trips_dim_table    FULL JOIN df_stations_dim_table ON df_stations_dim_table.station_id = df_trips_dim_table.start_station_id  FULL JOIN df_payment_dim_table ON  df_trips_dim_table.member_id = df_payment_dim_table.rider_id  FULL JOIN df_riders_dim_table ON df_riders_dim_table.rider_id = df_payment_dim_table.rider_id WHERE  df_trips_dim_table.trip_id IS NOT NULL AND member_id IS NOT NULL AND df_trips_dim_table.end_station_id IS NOT NULL  AND df_trips_dim_table.start_station_id IS NOT NULL")
fact_table.createOrReplaceTempView("fact_table")
#fact_table.display()

# fact_table_without_tripId= spark.sql("SELECT df_riders_dim_table.rider_id,  df_riders_dim_table.age AS age, df_trips_dim_table.start_station_id, df_trips_dim_table.end_station_id, df_trips_dim_table.member_id as trips_member_id, SUM(df_trips_dim_table.Trip_Duration_Minutes) AS Trip_Duration_Minutes , SUM(df_payment_dim_table.amount) AS amount  FROM  df_trips_dim_table FULL JOIN df_stations_dim_table ON df_stations_dim_table.station_id = df_trips_dim_table.start_station_id          FULL JOIN df_payment_dim_table ON df_trips_dim_table.member_id = df_payment_dim_table.rider_id  FULL JOIN df_riders_dim_table ON df_riders_dim_table.rider_id = df_payment_dim_table.rider_id GROUP BY  age, df_trips_dim_table.member_id, df_riders_dim_table.rider_id,  df_trips_dim_table.end_station_id, df_trips_dim_table.start_station_id HAVING member_id IS NOT NULL AND df_trips_dim_table.end_station_id IS NOT NULL  AND df_trips_dim_table.start_station_id IS NOT NULL").show()

# COMMAND ----------

# DBTITLE 1,Analyze how much time is spent per ride:
# MAGIC %md
# MAGIC 
# MAGIC ● Based on date and time factors such as day of week and time of day
# MAGIC 
# MAGIC ● Based on which station is the starting and / or ending station
# MAGIC 
# MAGIC ● Based on age of the rider at time of the ride
# MAGIC 
# MAGIC ● Based on whether the rider is a member or a casual rider

# COMMAND ----------

# DBTITLE 1,Answer: 01  Time is spent per ride:  Based on day of week and time of day
# df_trips.createOrReplaceTempView("df_trips_dim_table")
df_trips_dim_table_answer_01= spark.sql("SELECT df_trips_dim_table.week_day_name, SUM(df_trips_dim_table.Trip_Duration_Minutes) AS Trip_Duration_Minutes_sum  FROM df_trips_dim_table GROUP BY week_day_name SORT BY Trip_Duration_Minutes_sum DESC ")
df_trips_dim_table_answer_01.display(2)

#****************************************************************

###01:  Analyse how much time is spent per ride: Based on day of week and time of day 
import matplotlib.pyplot as plt
%matplotlib inline
import pandas as pd
import numpy as np


df_trips_dim_table_pd= df_trips_dim_table_answer_01.toPandas()
plt.bar(df_trips_dim_table_pd["week_day_name"], df_trips_dim_table_pd["Trip_Duration_Minutes_sum"])
# Set the title and labels
plt.title('week_day_name vs Trip_Duration_Minutes_sum')
plt.xlabel('week_day_name Group')
plt.ylabel('Trip_Duration_Minutes_sum')

# Show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1, Answer 2: Time spent Based on which station is the starting and / or ending station
from pyspark.sql.functions import sum, col, desc

TimeSpent_vs_start_station= spark.sql("SELECT start_station_id, sum(Trip_Duration_Minutes) as sum_Trip_Duration_Minutes FROM fact_table GROUP BY start_station_id HAVING start_station_id IS NOT NULL ORDER BY sum_Trip_Duration_Minutes DESC")
TimeSpent_vs_start_station.display()

TimeSpent_vs_end_station= spark.sql("SELECT end_station_id, sum(Trip_Duration_Minutes) as sum_Trip_Duration_Minutes FROM fact_table GROUP BY end_station_id HAVING end_station_id IS NOT NULL ORDER BY sum_Trip_Duration_Minutes DESC")
TimeSpent_vs_end_station.display()

# COMMAND ----------

# DBTITLE 1,Answer 3: Time spent Based on age of the rider at time of the ride
TimeSpent_vs_age_answer_03= spark.sql("select SUM(Trip_Duration_Minutes) AS Trip_Duration_Minutes, member_id, age  from fact_table GROUP BY member_id, age SORT BY Trip_Duration_Minutes DESC ")
TimeSpent_vs_age_answer_03.display()


#Convert table to pandas table for visualization
TimeSpent_vs_age_answer_03_pd = TimeSpent_vs_age_answer_03.toPandas()

# Create a bar chart of the Age vs Trip_Duration_Minutes
plt.bar(TimeSpent_vs_age_answer_03_pd["age"], TimeSpent_vs_age_answer_03_pd["Trip_Duration_Minutes"])
# Set the title and labels
plt.title('Age vs Trip_Duration_Minutes')
plt.xlabel('Age Group')
plt.ylabel('Trip_Duration_Minutes')

# Show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1,Answer: 4 Based on whether the rider is a member or a casual rider
timeSpent_vs_memberStatus_answer_04= spark.sql("select SUM(fact_table.Trip_Duration_Minutes) AS Trip_Duration_Minutes_sum, df_riders_dim_table.is_member from fact_table FULL JOIN df_riders_dim_table ON fact_table.member_id = df_riders_dim_table.rider_id GROUP BY df_riders_dim_table.is_member SORT BY Trip_Duration_Minutes_sum DESC ")
timeSpent_vs_memberStatus_answer_04.display()

# COMMAND ----------

# DBTITLE 1,Analyse how much money is spent:
# MAGIC %md
# MAGIC Q5: Per month, quarter, year
# MAGIC 
# MAGIC Q6: Per member, based on the age of the rider at account start

# COMMAND ----------

# DBTITLE 1,Answer 5: Analyse how much money is spent ● Per month, quarter, year
#
###
#
#####   NOT USING FACT TABLE HERE AS IT IS TAKING LONGEST TIME TO RUN  
#
###
#
###

# #Analyse how much money is spent Per month
moneySpent_perMonth_answer_05_01= spark.sql("select sum(amount) AS sum_of_amount, month_date as payment_month from df_payment_dim_table GROUP BY payment_month  SORT BY  payment_month desc")
# #answer_05_01.display()
moneySpent_perMonth_answer_05_01toPD = moneySpent_perMonth_answer_05_01.toPandas()
plt.bar(moneySpent_perMonth_answer_05_01toPD['payment_month'], moneySpent_perMonth_answer_05_01toPD['sum_of_amount'])
plt.title('Money spent per month')
plt.xlabel('Month')
plt.ylabel('Money spent')
plt.show()


# #Analyse how much money is spent quarter
moneySpent_perQuarter_answer_05_02= spark.sql("select sum(amount) AS sum_of_amount, quarter_date as payment_quarter from df_payment_dim_table GROUP BY payment_quarter SORT BY payment_quarter asc")
# #answer_05_02.display()
moneySpent_perquarter_answer_05_02toPD = moneySpent_perQuarter_answer_05_02.toPandas()
plt.bar(moneySpent_perquarter_answer_05_02toPD['payment_quarter'], moneySpent_perquarter_answer_05_02toPD['sum_of_amount'])
plt.title('Money spent per quarter')
plt.xlabel('Quarter')
plt.ylabel('Money spent')
plt.show()

##############################################Visualization##########

# #Analyse how much money is spent year
moneySpent_perYear_answer_05_03= spark.sql("select sum(amount) AS sum_of_amount, year_date as payment_year from df_payment_dim_table GROUP BY payment_year SORT BY payment_year asc")
# #answer_05_03.display()
moneySpent_perYear_answer_05_03toPD = moneySpent_perYear_answer_05_03.toPandas()
plt.bar(moneySpent_perYear_answer_05_03toPD['payment_year'], moneySpent_perYear_answer_05_03toPD['sum_of_amount'])
plt.title('Money spent per year')
plt.xlabel('Year')
plt.ylabel('Money spent')
plt.show()

# COMMAND ----------

# DBTITLE 1,Answer 6 : Analyze how much money is spent: Per member, based on the age of the rider at account start
#### 
rider_plus_payment_per_member_answer06= spark.sql("select fact_table.rider_id, df_riders_dim_table.account_start_date, fact_table.age, fact_table.amount from df_riders_dim_table FULL JOIN fact_table ON fact_table.payment_id = df_riders_dim_table.rider_id where fact_table.age IS NOT NULL AND fact_table.rider_id IS NOT NULL AND df_riders_dim_table.account_start_date IS NOT NULL ORDER BY df_riders_dim_table.age DESC, df_riders_dim_table.account_start_date DESC")
rider_plus_payment_per_member_answer06.display()


rider_plus_payment= spark.sql("select age, sum(amount) as amount from fact_table group by age HAVING age IS NOT NULL ORDER BY age DESC")

rider_plus_payment_toPd= rider_plus_payment.toPandas()
plt.bar(rider_plus_payment_toPd['age'], rider_plus_payment_toPd['amount'])
plt.title('Age(Grouped) vs Total Amount')
plt.xlabel('Age(Grouped)')
plt.ylabel('Sum of total Amount')
plt.show()


# COMMAND ----------

# DBTITLE 1,Analyze how much money is spent per member:
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Q7: Based on how many rides the rider averages per month
# MAGIC 
# MAGIC Q8 : Based on how many minutes the rider spends on a bike per month

# COMMAND ----------

# DBTITLE 1,Answer 7: Money is spent per member: Based on num of rides the rider averages per month
money_spent_perMember_perMonth= spark.sql("select fact_table.rider_id , SUM(df_payment_dim_table.amount), df_payment_dim_table.month_date as payment_month_date from df_payment_dim_table inner join fact_table ON fact_table.member_id = df_payment_dim_table.rider_id GROUP BY  fact_table.rider_id, df_payment_dim_table.month_date  ORDER BY fact_table.rider_id DESC, df_payment_dim_table.month_date DESC ")
money_spent_perMember_perMonth.display()


# COMMAND ----------

# DBTITLE 1,Question 8: how much money spent:  Based on how many minutes the rider spends on a bike per month
payment_plus_trips_plus_trips_08 = spark.sql("select rider_id, SUM(Trip_Duration_Minutes) AS trip_duration_minutes, SUM(amount) as amount  from fact_table GROUP BY rider_id HAVING rider_id IS NOT NULL AND Trip_Duration_Minutes IS NOT NULL SORT BY trip_duration_minutes DESC ")
payment_plus_trips_plus_trips_08.display()


# payment_plus_trips_plus_trips_08_toPd= payment_plus_trips_plus_trips_08.toPandas()
# payment_plus_trips_plus_trips_08_toPd.describe()
# plt.bar(payment_plus_trips_plus_trips_08_toPd['Trip_Duration_Minutes'], payment_plus_trips_plus_trips_08_toPd['rider_id'])
# plt.title('rider_id vs Sum of Trip_Duration_Minutes')
# plt.xlabel('rider_id')
# plt.ylabel('Trip_Duration_Minutes')
# plt.show()


