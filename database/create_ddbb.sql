-- Crear la base de datos
CREATE DATABASE geo_housing_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Crear un nuevo usuario con contraseña (credeciales de ejemplo)
CREATE USER geo_user WITH PASSWORD 'geo_password';

-- Otorgar privilegios al usuario sobre la base de datos
GRANT CONNECT ON DATABASE geo_housing_db TO geo_user;

-- Cambiar de base de datos 
\c geo_housing_db

-- Habilitar la extensión PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Crear las tablas
CREATE TABLE ccaas (
    ccaa_id INT PRIMARY KEY,
    ccaa_name VARCHAR(100) NOT NULL
);

CREATE TABLE provinces (
    province_id INT PRIMARY KEY,
    province_name VARCHAR(100) NOT NULL,
    fetched_pages INT DEFAULT 0,
    total_pages INT DEFAULT 0,
    is_fetched BOOLEAN DEFAULT FALSE,
    ccaa_id INT NOT NULL,
    FOREIGN KEY (ccaa_id) REFERENCES ccaas(ccaa_id)
);

CREATE TABLE cities (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    province_id INT NOT NULL,
    FOREIGN KEY (province_id) REFERENCES provinces(province_id)
);

CREATE TABLE variables (
    variable_id INT PRIMARY KEY,
    variable_name VARCHAR(50) NOT NULL,
    measurement_unit VARCHAR(10) NOT NULL
);

CREATE TABLE series_data (
    variable_id INT NOT NULL,
    city_id INT NOT NULL,
    date DATE NOT NULL,
    value FLOAT NOT NULL,
    is_historical BOOLEAN NOT NULL,
    PRIMARY KEY (variable_id, city_id, date),
    FOREIGN KEY (variable_id) REFERENCES variables(variable_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE ads_data (
    ad_id INT PRIMARY KEY,
    page_number INT NOT NULL,
    price INT NOT NULL,
    surface INT,
    rooms INT,
    bathrooms INT,
    zip_code INT,
    location GEOGRAPHY(Point, 4326),
    conservation_status INT,
    antiquity INT,
    floor_type INT,
    orientation INT,
    terrace BOOLEAN,
    parking BOOLEAN,
    elevator BOOLEAN,
    swimming_pool BOOLEAN,
    garden BOOLEAN,
    air_conditioner BOOLEAN,
    heater BOOLEAN,
    balcony BOOLEAN,
    bus_distance INT,
    train_distance INT,
    tram_distance INT,
    city_id INT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Otorgar todos los privilegios al nuevo usuario en esta base de datos
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO geo_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO geo_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO geo_user;
