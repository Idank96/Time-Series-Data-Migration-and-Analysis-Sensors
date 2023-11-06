ALTER TABLE Readings_raw
ALTER COLUMN TimeStamp TYPE TIMESTAMP WITH TIME ZONE USING TimeStamp AT TIME ZONE 'UTC';

INSERT INTO locations (LocationID, LocationName)
SELECT LocationID, LocationName  
FROM public."Locations_raw";

INSERT INTO sensors (SensorID, LocationID)
SELECT DISTINCT SensorID, LocationID 
FROM public."Sensors_raw";

INSERT INTO readings (SensorID, TimeStamp, ReadingValue)
SELECT SensorID, CAST(TimeStamp AS timestamp with time zone), ReadingValue
FROM public."Readings_raw";

