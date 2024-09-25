create schema if not exists staging;

drop table if exists staging.mongo_clients;
create table staging.mongo_clients(
	_id text primary key,
	name text,
	phone text,
	birthday text,
	email text,
	login text,
	address text,
	update_time timestamp,
	inserted_at timestamp,
	flag text
);

drop table if exists staging.mongo_orders;
create table staging.mongo_orders(
	_id text primary key,
	restaurant text,
	order_date timestamp,
	client text,
	ordered_dish text,
	payed_by_bonuses text,
	cost text,
	payment text,
	bonus_for_visit text,
	statuses text,
	final_status text,
	update_time timestamp,
	inserted_at timestamp,
	flag text
);

drop table if exists staging.mongo_restaurants;
create table staging.mongo_restaurants(
	_id text primary key,
	name text,
	phone text,
	email text,
	founding_day text,
	menu text,
	update_time timestamp,
	inserted_at timestamp,
	flag text
);

drop table if exists staging.pg_category;
create table staging.pg_category(
	category_id text primary key,
    name        varchar(255),
    percent     text,
    min_payment text,
    inserted_at timestamp,
    flag text
);

drop table if exists staging.pg_client;
create table staging.pg_client(
	client_id     text primary key,
    bonus_balance text,
    category_id   text,
    inserted_at timestamp,
    flag text
);

drop table if exists staging.pg_dish;
create table staging.pg_dish
(
    dish_id text primary key,
    name    text,
    price   text,
    inserted_at timestamp,
    flag text
);

drop table if exists staging.pg_payment;
create table staging.pg_payment
(
    payment_id  text primary key,
    client_id   text,
    dish_id     text,
    dish_amount text,
    order_id    text,
    order_time  timestamp,
    order_sum   text,
    tips        text,
    inserted_at timestamp,
    flag text
);

drop table if exists staging.api_deliveryman;
create table staging.api_deliveryman(
	_id text primary key,
	name text,
	inserted_at timestamp,
	flag text
);

drop table if exists staging.api_delivery;
create table staging.api_delivery(
	order_id text primary key,
	order_date_created timestamp,
	delivery_id text,
	deliveryman_id text,
	delivery_address text,
	delivery_time timestamp,
	rating text,
	tips text,
	inserted_at timestamp,
	flag text
);

drop table if exists staging.settings;
create table staging.settings
(
    postgres_actual_time timestamp,
    mongo_actual_time timestamp
);