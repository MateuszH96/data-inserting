CREATE SCHEMA IF NOT EXISTS weather;
--create tables fo database
DROP TABLE IF EXISTS weather.json_data_weather;
CREATE TABLE weather.json_data_weather(
	id_json SERIAL,
	weather_data JSON
);
DROP TABLE IF EXISTS weather.dates;
CREATE TABLE weather.dates(
	id_date TEXT,
	date_year SMALLINT,
	date_month SMALLINT,
	date_day SMALLINT,
	date_hour SMALLINT
	
);
DROP TABLE IF EXISTS weather.city;
CREATE TABLE weather.city(
	id_archive VARCHAR(9),
	city VARCHAR(30),
	id_current VARCHAR(6)
);
DROP TABLE IF EXISTS weather.weather_data;
CREATE TABLE weather.weather_data(
	id_json BIGSERIAL,
	id_date VARCHAR(10),
	id_station VARCHAR(9),
	temperature NUMERIC(10,2),
	wind_speed INT,
	wind_direction SMALLINT,
	humidity NUMERIC(10,2),
	pressure NUMERIC(12,2),
	precipitation NUMERIC(10,3)
);
--Functions and Triggers for inserting data into JSON DATES
--*********************************************************
CREATE OR REPLACE FUNCTION trigger_to_set_values_date()
RETURNS TRIGGER
LANGUAGE 'plpgsql'
AS $$
DECLARE
BEGIN
	UPDATE weather.dates d --(date_year,date_month,date_day,date_hour)
		SET date_year = (SUBSTRING(NEW.id_date,1,4))::SMALLINT,
		date_month = (SUBSTRING(NEW.id_date,5,2))::SMALLINT,
		date_day = (SUBSTRING(NEW.id_date,7,2))::SMALLINT,
		date_hour = (SUBSTRING(NEW.id_date,9,4))::SMALLINT
		WHERE d.id_date = NEW.id_date;
	RETURN NEW;
END;
$$;


CREATE TRIGGER convert_date_to_vals
AFTER INSERT 
ON weather.dates
FOR EACH ROW
EXECUTE PROCEDURE trigger_to_set_values_date();
--*********************************************************
--Functions and Triggers for inserting data into JSON DATES


--Functions and Triggers for inserting data into JSON DATA WEATHER
--****************************************************************
CREATE OR REPLACE FUNCTION change_to_id_date(date_val VARCHAR(12),hour_val VARCHAR(4))
RETURNS VARCHAR(10)
LANGUAGE 'plpgsql'
AS $$
DECLARE
	to_return VARCHAR(10);
	connected VARCHAR(16);
BEGIN
	if LENGTH(hour_val)=3 then
		SELECT SUBSTRING(hour_val,1,1)||0||SUBSTRING(hour_val,2)INTO hour_val;
	end if;
	SELECT date_val||hour_val INTO connected;
	SELECT TRANSLATE(connected,'"','') INTO connected;
	SELECT TRANSLATE(connected,'-','') INTO to_return;
	RETURN to_return;
END;
$$;

CREATE OR REPLACE FUNCTION change_character(string_to_change TEXT,charcter_to_change VARCHAR(1),replace_character VARCHAR(1))
RETURNS TEXT
LANGUAGE 'plpgsql'
AS $$
DECLARE
	to_return TEXT;
BEGIN
	SELECT TRANSLATE(string_to_change, charcter_to_change, replace_character) INTO to_return;
	RETURN to_return;
END;
$$;

CREATE OR REPLACE FUNCTION trigger_inserted_values()
RETURNS TRIGGER
LANGUAGE 'plpgsql'
AS $$
DECLARE
	date_var VARCHAR(12);
	hour_var VARCHAR(4);
	date_id VARCHAR(10);
	id_station_var TEXT;
	temperature_var TEXT;
	wind_speed_var TEXT;
	wind_dir_var TEXT;
	humidity_var TEXT;
	pressure_var TEXT;
	precipitation_var TEXT;
BEGIN
	-- Inserting data to dates table
	SELECT weather_data -> 'data_pomiaru' 
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO date_var;
		
	SELECT weather_data -> 'godzina_pomiaru' 
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO hour_var;
		
	SELECT change_to_id_date(date_var,hour_var) INTO date_id;
	INSERT INTO weather.dates (id_date) 
		SELECT date_id 
		WHERE NOT EXISTS(
			SELECT id_date 
			FROM weather.dates d 
			WHERE d.id_date = date_id
		);
	--Inserting data to weather_data
	SELECT weather_data -> 'id_stacji'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO id_station_var;
		
	SELECT weather_data -> 'temperatura'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO temperature_var;
		
	SELECT weather_data -> 'predkosc_wiatru'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO wind_speed_var;
		
	SELECT weather_data -> 'kierunek_wiatru'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO wind_dir_var;
		
	SELECT weather_data -> 'wilgotnosc_wzgledna'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO humidity_var;
			
	SELECT weather_data -> 'cisnienie'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO pressure_var;
		
	SELECT weather_data -> 'suma_opadu'
		FROM weather.json_data_weather w 
		WHERE w.id_json = NEW.id_json
		INTO precipitation_var;
		
	INSERT INTO weather.weather_data(id_json,id_date,id_station,temperature,wind_speed,wind_direction,humidity,pressure,precipitation) 
		VALUES(
			NEW.id_json,
			date_id,
			change_character(id_station_var,'"','')::VARCHAR(9),
			change_character(temperature_var,'"','')::NUMERIC(10,2),
			change_character(wind_speed_var,'"','')::INT,
			change_character(wind_dir_var,'"','')::SMALLINT,
			change_character(humidity_var,'"','')::NUMERIC(10,2),
			change_character(pressure_var,'"','')::NUMERIC(12,2),
			change_character(precipitation_var,'"','')::NUMERIC(10,3)
		);
	RETURN NEW;
END;
$$;

CREATE TRIGGER json_input_val 
AFTER INSERT 
ON weather.json_data_weather
FOR EACH ROW
EXECUTE PROCEDURE trigger_inserted_values();
--****************************************************************
--Functions and Triggers for inserting data into JSON DATA WEATHER

--TEST INSERTING DATA
/*INSERT INTO weather.json_data_weather (weather_data) VALUES(
'{"id_stacji":"12295","stacja":"Bia\u0142ystok","data_pomiaru":"2023-04-08","godzina_pomiaru":"10","temperatura":"10.1","predkosc_wiatru":"2","kierunek_wiatru":"320","wilgotnosc_wzgledna":"59.7","suma_opadu":"0.01","cisnienie":"1025.3"}'
);
INSERT INTO weather.json_data_weather (weather_data) VALUES(
'{"id_stacji":"12295","stacja":"Bia\u0142ystok","data_pomiaru":"2023-04-08","godzina_pomiaru":"10","temperatura":"10.1","predkosc_wiatru":"2","kierunek_wiatru":"320","wilgotnosc_wzgledna":"59.7","suma_opadu":"0.01","cisnienie":"1025.3"}'
);*/

select * from weather.json_data_weather;
select * from weather.dates;
select * from weather.city;
select * from weather.weather_data;

