--  find the average, minimum, and maximum sensor readings for a specific sensor within a given time period.

SELECT SensorID,
       MAX(ReadingValue) AS max_reading_value,
       MIN(ReadingValue) AS min_reading_value,
       AVG(ReadingValue) AS average_reading_value
FROM readings
WHERE TimeStamp BETWEEN '2020-01-01 00:00:00' AND '2020-03-31 00:00:00'
GROUP BY SensorID
ORDER BY SensorID ASC;

-- find the sensors that experienced readings above a certain threshold value.
SELECT  DISTINCT SensorID as sensors_above_200
FROM readings
WHERE ReadingValue > 200
ORDER BY SensorID ASC;


-- Calculate the hourly, daily, and monthly averages for sensor
-- readings and store the results in appropriate tables.


-- Create daily table
CREATE TABLE daily_readings (
  SensorID INT NOT NULL,
  day INT NOT NULL,
  avg_readings FLOAT NOT NULL,
  PRIMARY KEY (SensorID, day)
);

-- Insert data into the table
INSERT INTO daily_readings
SELECT SensorID, 
	   EXTRACT(day from TimeStamp) AS day,
	   AVG(ReadingValue) AS avg_readings
FROM readings
GROUP BY SensorID, EXTRACT(day from TimeStamp);

-- Create hourly table
CREATE TABLE hourly_readings (
  SensorID INT NOT NULL,
  hour INT NOT NULL,
  avg_readings FLOAT NOT NULL,
  PRIMARY KEY (SensorID, hour)
);

-- Insert data into the table
INSERT INTO hourly_readings
SELECT SensorID, 
	   EXTRACT(hour from TimeStamp) AS hour,
	   AVG(ReadingValue) AS avg_readings
FROM readings
GROUP BY SensorID, EXTRACT(hour from TimeStamp);


-- Create monthly table
CREATE TABLE monthly_readings (
  SensorID INT NOT NULL,
  month INT NOT NULL,
  avg_readings FLOAT NOT NULL,
  PRIMARY KEY (SensorID, month)
);

-- Insert data into the table
INSERT INTO monthly_readings
SELECT SensorID, 
	   EXTRACT(month from TimeStamp) AS month,
	   AVG(ReadingValue) AS avg_readings
FROM readings
GROUP BY SensorID, EXTRACT(month from TimeStamp);