create schema if not exists source;

drop table if exists source.category cascade;
create table source.category
(
    category_id serial primary key,
    name        varchar(255),
    percent     numeric(10, 2),
    min_payment numeric(10, 2)
);

drop table if exists source.client;
create table source.client
(
    client_id     serial primary key,
    bonus_balance numeric(10, 2),
    category_id   int
);

drop table if exists source.dish;
create table source.dish
(
    dish_id serial primary key,
    name    varchar(255),
    price   numeric(10, 2)
);
      
drop table if exists source.payment;
create table source.payment
(
    payment_id  serial primary key,
    client_id   int,
    dish_id     int,
    dish_amount int,
    order_id    int,
    order_time  timestamp,
    order_sum   numeric(10, 2),
    tips        numeric(10, 2)
);

drop table if exists source.logs;
create table source.logs
(
    id         serial
        constraint logs_pk
            primary key,
    table_name varchar(255),
    time       timestamp default current_timestamp,
    values     jsonb
);

CREATE OR REPLACE FUNCTION source.log_changes()
    RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO source.logs (table_name, values)
    VALUES (TG_TABLE_NAME, row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER category_logs_trigger
    AFTER INSERT OR UPDATE
    ON source.category
    FOR EACH ROW
EXECUTE FUNCTION source.log_changes();

CREATE TRIGGER client_logs_trigger
    AFTER INSERT OR UPDATE
    ON source.client
    FOR EACH ROW
EXECUTE FUNCTION source.log_changes();

CREATE TRIGGER dish_logs_trigger
    AFTER INSERT OR UPDATE
    ON source.dish
    FOR EACH ROW
EXECUTE FUNCTION source.log_changes();

CREATE TRIGGER payment_logs_trigger
    AFTER INSERT OR UPDATE
    ON source.payment
    FOR EACH ROW
EXECUTE FUNCTION source.log_changes();