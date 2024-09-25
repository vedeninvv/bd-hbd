INSERT INTO dds.Dim_Clients (client_id, name, phone, birthday, email, login, address)
SELECT _id, name, phone, birthday, email, login, address
FROM staging.mongo_clients;

-- Load Dim_Restaurants
INSERT INTO dds.Dim_Restaurants (restaurant_id, name, phone, email, founding_day)
SELECT _id, name, phone, email, founding_day
FROM staging.mongo_restaurants;

-- Load Dim_Orders
INSERT INTO dds.Dim_Orders (order_id, payed_by_bonuses, cost, payment, bonus_for_visit, final_status)
SELECT _id, payed_by_bonuses, cost, payment, bonus_for_visit, final_status
FROM staging.mongo_orders;

-- Load Dim_Delivery
INSERT INTO dds.Dim_Delivery (delivery_id, delivery_address, delivery_time, order_date_created, rating, tips)
SELECT delivery_id, delivery_address, delivery_time, order_date_created, rating, tips
FROM staging.api_delivery;

-- Load Dim_Deliveryman
INSERT INTO dds.Dim_Deliveryman (id, name)
SELECT _id, name
FROM staging.api_deliveryman;

-- Load Dim_Time (simplified example for demo purposes)
INSERT INTO dds.Dim_Time (time_mark, year, month, day, time)
SELECT 
    order_date_created,
    EXTRACT(YEAR FROM order_date_created)::SMALLINT,
    EXTRACT(MONTH FROM order_date_created)::SMALLINT,
    EXTRACT(DAY FROM order_date_created)::SMALLINT,
    CAST(order_date_created as TIME)
FROM staging.api_delivery;

-- Load Fact_Table
INSERT INTO dds.Fact_Table (delivery_id, date_id, client_id, restaurant_id, deliveryman_id, order_id, valid_from, valid_to)
SELECT 
    api_delivery.delivery_id,
    Dim_Time.id,
    mongo_orders.client,
    mongo_orders.restaurant,
    api_delivery.deliveryman_id,
    api_delivery.order_id,
    api_delivery.order_date_created,
    NULL
FROM 
    staging.api_delivery
    JOIN staging.mongo_orders ON api_delivery.order_id = mongo_orders._id
    JOIN dds.Dim_Time ON api_delivery.order_date_created = Dim_Time.time_mark;