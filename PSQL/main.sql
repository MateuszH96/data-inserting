
-- tworzenie tabeli zawirające daty
DROP TABLE IF EXISTS weather.dates;
CREATE TABLE weather.dates(
	id_date VARCHAR(10) PRIMARY KEY,
	date_year SMALLINT,
	date_month SMALLINT,
	date_day SMALLINT,
	date_hour SMALLINT
	
);
--tworzenie tabeli z miastami
DROP TABLE IF EXISTS weather.city;
CREATE TABLE weather.city(
	id_archive VARCHAR(9) PRIMARY KEY,
	city VARCHAR(30),
	id_current VARCHAR(6)
);
--tworzenie tabeli z danymi pogodowymi
DROP TABLE IF EXISTS weather.weather_data;
CREATE TABLE weather.weather_data(
	id_date VARCHAR(10),
	id_station VARCHAR(9),
	temperature NUMERIC(10,2),
	wind_speed NUMERIC(10,2),
	wind_direction SMALLINT,
	humidity NUMERIC(10,2),
	pressure NUMERIC(12,2),
	precipitation NUMERIC(10,3)
);

CREATE OR REPLACE FUNCTION create_date_convert()
RETURNS TRIGGER
LANGUAGE 'plpgsql'
AS $$
BEGIN
	INSERT INTO weather.dates(id_date) 
	SELECT NEW.id_date 
	WHERE NOT EXISTS(
		SELECT *
		FROM weather.dates d 
		WHERE d.id_date=NEW.id_date
	);
	RETURN NEW;
END;
$$;

CREATE TRIGGER create_date
AFTER INSERT ON weather.weather_data
FOR EACH ROW
EXECUTE FUNCTION create_date_convert();

DROP TABLE IF EXISTS weather.hourly_json; 
-- taworzenei tabeli z danymi cogodzinnymi
CREATE TABLE weather.hourly_json (
  id BIGSERIAL,
  json_data JSON
);

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

CREATE OR REPLACE FUNCTION hour_conversion(val TEXT)
RETURNS VARCHAR(2) AS $$
DECLARE
	to_return VARCHAR(2);
BEGIN
	to_return := val::VARCHAR(2);
	IF LENGTH(val)=1 THEN
	to_return := '0' || val::VARCHAR(1);
	END IF;
	RETURN to_return;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION date_conversion(val TEXT)
RETURNS VARCHAR(8) AS $$
DECLARE
	to_return VARCHAR(8);
BEGIN
	to_return := TRANSLATE(val,'-','')::VARCHAR(8);
	IF LENGTH(to_return) = 7 THEN
	to_return := SUBSTRING(to_return FROM 1 FOR LENGTH(to_return)-1) || '0' || SUBSTRING(to_return FROM LENGTH(to_return));
	END IF;
	RETURN to_return;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION json_data_convert() 
RETURNS TRIGGER AS $$
BEGIN
    --INSERT INTO weather.weather_data (id_stacji, temperatura,predkosc_wiatru,kierunek_wiatru,wilgotnosc_wzgledna,suma_opadu,cisnienie,id_daty)
    INSERT INTO weather.weather_data(id_station,temperature,wind_speed,wind_direction,humidity,pressure,precipitation,id_date)
	SELECT 
	  (json_rec->>'id_stacji')::VARCHAR(10),
	  (json_rec->>'temperatura')::numeric,
	  (json_rec->>'predkosc_wiatru')::numeric,
	  (json_rec->>'kierunek_wiatru')::numeric,
	  (json_rec->>'wilgotnosc_wzgledna')::numeric,
	  (json_rec->>'suma_opadu')::numeric,
	  (json_rec->>'cisnienie')::numeric,
	  (SELECT(date_conversion(json_rec->>'data_pomiaru')::text) || hour_conversion((json_rec->>'godzina_pomiaru')::text) )
	FROM jsonb_array_elements(NEW.json_data::jsonb) AS json_rec;
	  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Tworzenie triggera
CREATE TRIGGER convert_json_data_to_rows
AFTER INSERT ON weather.hourly_json
FOR EACH ROW
EXECUTE FUNCTION json_data_convert();

CREATE INDEX id_date_index ON weather.weather_data(id_date);
CREATE INDEX id_station_index ON weather.weather_data(id_station);
CREATE INDEX id_year_index ON weather.dates(date_year);
CREATE INDEX id_month_index ON weather.dates(date_month);
CREATE INDEX id_day_index ON weather.dates(date_day);
CREATE INDEX id_hour_index ON weather.dates(date_hour);


INSERT INTO weather.weather_data(id_station,temperature,wind_speed,wind_direction,humidity,pressure,precipitation,id_date)
	
	--stworzyć "with"
	SELECT 
		(weather_data->>'id_stacji')::VARCHAR(9),
		(weather_data->>'temperatura')::NUMERIC(10,2),
		(weather_data->>'predkosc_wiatru')::NUMERIC(10,2), 
		(weather_data->>'kierunek_wiatru')::SMALLINT,
		(weather_data->>'wilgotnosc_wzgledna')::NUMERIC(10,2),
		(weather_data->>'cisnienie')::NUMERIC(12,2),
		(weather_data->>'suma_opadu')::NUMERIC(10,3),
		(SELECT(date_conversion(weather_data->>'data_pomiaru')::TEXT)||hour_conversion((weather_data->>'godzina_pomiaru')::TEXT))
	FROM weather.json_data_weather;


WITH zero_pressure_id AS (
	SELECT w.id_date
	FROM weather.weather_data w
	WHERE w.pressure < 966
)
UPDATE weather.weather_data w
	SET pressure = (
		SELECT AVG(p.pressure)
		FROM weather.weather_data p
		INNER JOIN zero_pressure_id z ON z.id_date = p.id_date
		WHERE p.pressure >= 966
	)
WHERE w.id_date IN (SELECT id_date FROM zero_pressure_id) AND w.pressure <966;

DROP TABLE IF EXISTS weather.average_year_data;
CREATE TABLE weather.average_year_data as(
	WITH connected_table as(
	SELECT * FROM weather.weather_data wd
	INNER JOIN weather.dates d ON d.id_date = wd.id_date
	INNER JOIN weather.city c ON c.id_archive = wd.id_station
	)

	SELECT ct.city,ct.date_year, AVG(ct.temperature) as avg_temperature, AVG(ct.pressure) as avg_pressure, AVG(ct.wind_speed) as avg_wind_speed,AVG(ct.humidity) as avg_humidity,AVG(ct.precipitation) as avg_precipitation
	FROM connected_table ct
	GROUP BY ct.city, ct.date_year
);

CREATE OR REPLACE FUNCTION create_date_convert()
RETURNS TRIGGER
LANGUAGE 'plpgsql'
AS $$
BEGIN
	INSERT INTO weather.dates(id_date) 
	SELECT NEW.id_date 
	WHERE NOT EXISTS(
		SELECT *
		FROM weather.dates d 
		WHERE d.id_date=NEW.id_date
	);
	WITH connected_table as(
	SELECT * FROM weather.weather_data wd
	INNER JOIN weather.dates d ON d.id_date = wd.id_date
	INNER JOIN weather.city c ON c.id_archive = wd.id_station
	WHERE NEW.id_station = wd.id_archive AND  NEW.id_date SIMILAR TO '%' || ct.date_year::TEXT ||'%'
	)
	
	UPDATE weather.average_year_data
		SET
		avg_pressure = (SELECT AVG(ct.pressure)FROM connected_table ct),
		avg_temperature = (SELECT AVG(ct.temperature)FROM connected_table ct),
		avg_humidity = (SELECT AVG(ct.humidity)FROM connected_table ct),
		avg_precipitation = (SELECT AVG(ct.precipitation)FROM connected_table ct),
		avg_wind_speed = (SELECT AVG(ct.wind_speed)FROM connected_table ct),

	RETURN NEW;
END;
$$;

CREATE TRIGGER create_date
AFTER INSERT ON weather.weather_data
FOR EACH ROW
EXECUTE FUNCTION create_date_convert();