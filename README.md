## Python + SQL data ingestion and analysis

![alt text](Diagram.drawio.png)
![alt text](flow.drawio.png)


> Assumptions:
> * Each sensor must be in at least one location.
> * A location does not have to have a sensor in it.
> * Each reading must come from only one sensor.
> * Each reading has a sensor.



This repository provides a Python and SQL pipeline for ingesting and analyzing sensor data. The pipeline consists of the following steps:

1. Convert CSV files to Parquet files.
2. Load Parquet files into PostgreSQL as raw data tables.
3. Transform the raw data tables into more concise and efficient tables for specific queries.
4. Write and run queries according to the given questions.

Here how I've done it:

**Day 1**

* Understand the task and ask questions.
* Design the graph of the tables.
* Write a Python and SQL pipeline to transform and load the data: CSV --> Parquet --> PostgreSQL tables.
* Write the 2nd and 3rd query tasks.

**Day 2**

* Write the fourth query:

    I chose to create a table for each "type of period": hour_readings, daily_readings, monthly_readings.
    
    The primary key of each table is a pair of sensorID and "type of period". This way, it would be easy to select a specific sensor and month (for example) and get the average reading of that month.

* Write the 4th and 5th query tasks.
* Insert lookup table and write query with it (not DONE yes).
* Convert all the queries to Python in Pandas.
* Write the ETL vs. ELT evaluation notebook.
* Create the requirements.txt file and README.md file.
