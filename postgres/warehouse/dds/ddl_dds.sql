create schema if not exists dds;

drop table if exists dds.dim_clients cascade;
CREATE TABLE dds.Dim_Clients (
    client_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    birthday TEXT NOT NULL,
    email TEXT NOT NULL,
    login TEXT NOT NULL,
    address TEXT NOT NULL
);

-- Dim_Restaurants
drop table if exists dds.dim_restaurants cascade;
CREATE TABLE dds.Dim_Restaurants (
    restaurant_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT NOT NULL,
    founding_day TEXT NOT NULL
);

-- Dim_Orders
drop table if exists dds.dim_orders cascade;
CREATE TABLE dds.Dim_Orders (
    order_id TEXT PRIMARY KEY NOT NULL,
    payed_by_bonuses TEXT NOT NULL,
    cost TEXT NOT NULL,
    payment TEXT NOT NULL,
    bonus_for_visit TEXT NOT NULL,
    final_status TEXT NOT NULL
);

-- Dim_Delivery
drop table if exists dds.dim_delivery cascade; 
CREATE TABLE dds.Dim_Delivery (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    delivery_address TEXT NOT NULL,
    delivery_time TIMESTAMP NOT NULL,
    order_date_created TIMESTAMP NOT NULL,
    rating TEXT,
    tips TEXT
);

-- Dim_Deliveryman
drop table if exists dds.dim_deliveryman cascade;
CREATE TABLE dds.Dim_Deliveryman (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);

-- Dim_Time
drop table if exists dds.dim_time cascade;
CREATE TABLE dds.Dim_Time (
    id SERIAL PRIMARY KEY,
    time_mark TIMESTAMP NOT NULL,
    year SMALLINT CONSTRAINT year CHECK (year > 2022),
    month SMALLINT CONSTRAINT month CHECK (month BETWEEN 1 AND 12),
    day SMALLINT CONSTRAINT day CHECK (day BETWEEN 1 AND 31),
    time TIME
);

-- Fact_Table
drop table if exists dds.fact_table;
CREATE TABLE dds.Fact_Table (
    id SERIAL PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    date_id INT NOT NULL,
    client_id TEXT NOT NULL,
    restaurant_id TEXT NOT NULL,
    deliveryman_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
);