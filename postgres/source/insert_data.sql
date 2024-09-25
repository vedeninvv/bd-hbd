insert into source.category(name, percent, min_payment)
VALUES ('Обычный', 20, 1000),
       ('ВИП', 15, 5000);

insert into source.client(bonus_balance, category_id)
VALUES (10.50, 1),
       (20, 2),
       (15.50, 1),
       (30, 2),
       (10, 1);

     
insert into source.dish(name, price)
VALUES ('Борщ', 350),
       ('Пельмени', 500),
       ('Чебуре', 600),
       ('Плов', 200),
       ('Рататуй', 350);


insert into source.payment(client_id, dish_id, dish_amount, order_id, order_time, order_sum, tips)
VALUES (1, 1, 3, 1, '2023-04-15 14:30:00', 1000, 150),
       (1, 1, 4, 2, '2023-04-15 20:30:00', 1200, 0),
       (2, 3, 1, 3, '2024-04-20 14:00:00', 500, 0),
       (3, 2, 3, 3, '2024-04-20 14:02:00', 600, 0),
       (5, 5, 1, 4, '2023-04-15 15:30:00', 1200, 400);
       