Data explanation:


Column: EventId

Meaning/Description: Unique identifier for each weather event record.
Data Type: String (alphanumeric).
Example Values: "W-1", "W-123456".


Column: Type

Meaning/Description: Category of the weather event (e.g., precipitation-related or visibility issues).
Data Type: String (categorical).
Example Values: "Rain", "Snow", "Fog", "Storm", "Cold", "Precipitation" (6 unique types overall).


Column: Severity

Meaning/Description: Intensity level of the weather event.
Data Type: String (categorical).
Example Values: "Light", "Moderate", "Heavy", "Severe", "UNK" (unknown), "Other" (6 unique levels).


Column: StartTime(UTC)

Meaning/Description: Start timestamp of the weather event in UTC.
Data Type: Datetime/Timestamp (parsed as string in CSV, convert in Spark/Snowflake).
Example Values: "2016-01-01 00:00:00", "2022-12-31 23:59:00".


Column: EndTime(UTC)

Meaning/Description: End timestamp of the weather event in UTC.
Data Type: Datetime/Timestamp (similar to StartTime).
Example Values: "2016-01-01 00:59:00", "2022-12-31 23:59:00" (events can span minutes to hours).


Column: Precipitation(in)

Meaning/Description: Amount of precipitation in inches during the event (key for your anomaly detection in the SQL view).
Data Type: Float (numeric, can be null if not applicable).
Example Values: 0.0, 0.12, 1.5 (ranges from 0 to high values in storms; average ~0.1 in rainy events).


Column: TimeZone

Meaning/Description: US time zone of the event location (for local time conversions if needed).
Data Type: String.
Example Values: "US/Pacific", "US/Eastern", "US/Central", "US/Mountain".


Column: AirportCode

Meaning/Description: ICAO code of the nearest airport weather station reporting the event (proxies for urban/rural areas).
Data Type: String (4 characters).
Example Values: "KLAX" (Los Angeles), "KJFK" (New York JFK), "KORD" (Chicago O'Hare).


Column: LocationLat

Meaning/Description: Latitude coordinate of the event location (for geospatial analysis per area).
Data Type: Float.
Example Values: 34.0522 (Los Angeles), 40.7128 (New York), -87.6298 (Chicago).


Column: LocationLng

Meaning/Description: Longitude coordinate of the event location.
Data Type: Float.
Example Values: -118.2437 (Los Angeles), -74.0060 (New York), 41.8781 (Chicago).


Column: City

Meaning/Description: Nearest city to the weather event (granularity within states).
Data Type: String.
Example Values: "Los Angeles", "New York", "Chicago" (~1,000 unique cities across the dataset).


Column: County

Meaning/Description: County within the state where the event occurred (finer area breakdown).
Data Type: String.
Example Values: "Los Angeles", "New York", "Cook" (thousands unique, tied to cities).


Column: State

Meaning/Description: US state abbreviation (core for your state-level aggregation).
Data Type: String (2 letters).
Example Values: "CA", "NY", "IL" (49 unique states).


Column: ZipCode

Meaning/Description: ZIP code of the event area (most granular for urban/suburban splits).
Data Type: String (5 digits, sometimes with extension).
Example Values: "90045", "10001", "60601" (many unique, but can be null in rural areas).