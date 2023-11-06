CREATE TABLE Sensors (
  SensorID INT NOT NULL,
  LocationID INT NOT NULL,
  PRIMARY KEY (SensorID)
);

CREATE TABLE Locations (
  LocationID INT NOT NULL,
  LocationName CHAR(256) NOT NULL,
  PRIMARY KEY (LocationID)
);

CREATE TABLE Readings (
  SensorID INT NOT NULL,
  TimeStamp TIMESTAMP NOT NULL,
  ReadingValue FLOAT NOT NULL,
  PRIMARY KEY (SensorID, TimeStamp),
  FOREIGN KEY (SensorID) REFERENCES Sensors (SensorID)
);

ALTER TABLE Sensors
ADD CONSTRAINT sensors_location_constraint
FOREIGN KEY (LocationID) REFERENCES Locations (LocationID);

-- Add lookup table and add locations constrain that its must match.
CREATE TABLE Locations_lookup (
  City VARCHAR(256) NOT NULL UNIQUE PRIMARY KEY
);

INSERT INTO Locations_lookup (City) VALUES
  ('Haifa'),
  ('Tel Aviv'),
  ('Jerusalem');

ALTER TABLE Locations
  ADD CONSTRAINT known_location_name
  FOREIGN KEY (LocationName)
  REFERENCES Locations_lookup (City);


