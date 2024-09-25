create schema if not exists cdm;

drop table if exists cdm.deliveryman_income;
CREATE TABLE cdm.deliveryman_income (
    id SERIAL PRIMARY KEY,
    deliveryman_id TEXT NOT NULL,
    deliveryman_name TEXT NOT NULL,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    orders_amount INT NOT NULL,
    orders_total_cost NUMERIC(10, 2) NOT NULL,
    rating NUMERIC(3, 2) NOT NULL,
    company_commission NUMERIC(10, 2) NOT NULL,
    deliveryman_order_income NUMERIC(10, 2) NOT NULL,
    tips NUMERIC(10, 2) NOT NULL
);

INSERT INTO cdm.deliveryman_income (
    deliveryman_id, deliveryman_name, year, month, orders_amount, orders_total_cost, rating, company_commission, deliveryman_order_income, tips
)
SELECT
    d.id AS deliveryman_id,
    d.name AS deliveryman_name,
    2024 AS year,
    12 AS month,
    COUNT(ft.id) AS orders_amount,
    SUM(o.cost::NUMERIC) AS orders_total_cost,
    AVG(dv.rating::NUMERIC) AS rating,
    SUM(o.cost::NUMERIC) * 0.5 AS company_commission,
    CASE
        WHEN AVG(dv.rating::NUMERIC) < 8 THEN
            CASE
                WHEN SUM(o.cost::NUMERIC) * 0.05 < 400 THEN 400
                ELSE SUM(o.cost::NUMERIC) * 0.05
            END
        WHEN AVG(dv.rating::NUMERIC) >= 10 THEN
            CASE
                WHEN SUM(o.cost::NUMERIC) * 0.1 > 1000 THEN 1000
                ELSE SUM(o.cost::NUMERIC) * 0.1
            END
        ELSE
            SUM(o.cost::NUMERIC) * 0.05
    END AS deliveryman_order_income,
    SUM(dv.tips::NUMERIC) AS tips
FROM
    dds.Fact_Table ft
    JOIN dds.Dim_Delivery dv ON ft.delivery_id = dv.delivery_id
    JOIN dds.Dim_Deliveryman d ON ft.deliveryman_id = d.id
    JOIN dds.Dim_Orders o ON ft.order_id = o.order_id
WHERE
    EXTRACT(YEAR FROM dv.delivery_time) = 2024
    AND EXTRACT(MONTH FROM dv.delivery_time) = 12
GROUP BY
    d.id, d.name;