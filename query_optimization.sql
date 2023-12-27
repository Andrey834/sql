-- 1 Задание                  ****************************
/*
1. Вставка новых данных подвисает из-за того что,
отсутствует инкремент на поле order_id.
Соответственно приходится использовать SELECT с агрегатной функцией
которая обрабатывает все строки таблицы.

2. Чтобы ускорить вставку элемента в таблицу orders:
    - Добавляем автоинкремент на поле order_id
    - Добавляем дефолтное значение к полю order_dt(Опционально)
    - Удаляем не используемые индексы
3*/
--Устанавливаем автоинкремент для поля order_id таблицы orders
--и добавляем DEFAULT VALUE для поля order_dt
CREATE SEQUENCE orders_order_id_seq;
SELECT SETVAL('orders_order_id_seq', (SELECT MAX(order_id) FROM orders));

alter table orders
    ALTER COLUMN order_id SET DEFAULT nextval('orders_order_id_seq'),
    ALTER COLUMN order_dt SET DEFAULT CURRENT_TIMESTAMP;

--Дропаем не используемые индексы
DROP INDEX
    orders_city_id_idx,
    orders_device_type_city_id_idx,
    orders_device_type_idx,
    orders_discount_idx,
    orders_final_cost_idx,
    orders_total_cost_idx,
    orders_total_final_cost_discount_idx;

--Убираем из запроса SELECT
BEGIN;
EXPLAIN ANALYZE
INSERT INTO orders
(user_id, device_type, city_id, total_cost, discount,
 final_cost)
VALUES ('329551a1-215d-43e6-baee-322f2467272d',
        'Mobile',
        1,
        1000.00,
        null,
        1000.00);
ROLLBACK;


-- 2 Задание                  ****************************
/*
1. Данные ищутся долго из-за того, что в таблице:
    -Не оптимальные типы для полей, из-за чего приходится приводить их к определенному типу
    -Отсутствуют индексы для полей которые которые задействованы в селекте

2. Чтобы ускорить поиск, можно:
    - Добавить индекс на поле city_id
    - Поменять типы у полей
    - Убрать из селекта преобразование к определнным типам
3*/
--Изменям тип полей
ALTER TABLE users
    ALTER COLUMN user_id TYPE uuid USING trim(user_id)::uuid,
    ALTER COLUMN first_name TYPE varchar(50),
    ALTER COLUMN last_name TYPE varchar(50),
    ALTER COLUMN city_id TYPE int,
    ALTER COLUMN gender TYPE varchar(6),
    ALTER COLUMN birth_date TYPE date USING trim(birth_date)::date,
    ALTER COLUMN registration_date TYPE timestamp USING trim(birth_date)::timestamp;

--Добавляем индекс на поле city_id таблицы users
CREATE INDEX
    users_city_id_idx ON users (city_id);

--Изменяем запрос
EXPLAIN ANALYZE
SELECT user_id,
       first_name,
       last_name,
       city_id,
       gender
FROM users
WHERE city_id = 4
  AND date_part('day', birth_date)
    = date_part('day', to_date('31-12-2023', 'dd-mm-yyyy'))
  AND date_part('month', birth_date)
    = date_part('month', to_date('31-12-2023', 'dd-mm-yyyy'));



-- 3 Задание                  ****************************
/*
1. Дублирование данных в двух таблицах. orders - sales
2. Решение:
    -Убрать  из процедуры взаимодействие с таблицей sales
    -Удалить таблицу sales
    -(Опционально) Изменить поля таблиц для дефолтных значений
3--*/
ALTER TABLE payments
    ALTER COLUMN payment_id SET DEFAULT nextval('payments_payment_id_sq'),
    ADD PRIMARY KEY (payment_id);
SELECT SETVAL('payments_payment_id_sq', (SELECT MAX(payment_id) FROM payments));

ALTER TABLE order_statuses
    ALTER COLUMN status_dt SET DEFAULT current_timestamp;

DROP PROCEDURE add_payment(p_order_id bigint, p_sum_payment numeric);
DROP TABLE sales;
DROP sequence sales_sale_id_sq;

CREATE PROCEDURE add_payment(IN p_order_id bigint, IN p_sum_payment numeric)
    language plpgsql
as
$$
BEGIN
    INSERT INTO order_statuses (order_id, status_id)
    VALUES (p_order_id, 2);

    INSERT INTO payments (order_id, payment_sum)
    VALUES (p_order_id, p_sum_payment);
END;
$$;


-- 4 Задание                  ****************************
/*
1. Слишком большой объем данных в таблице учитывая что основная работа ведется с данными за текующий квартал
2. Что-бы ускорить работу с таблицей, можно создать таблицу с декларативным партицированим по кварталам
3 Код для создания новой парт таблицы(user_logs_part) и переноса данных из (user_logs) с последующим удалением таблицы:*/

CREATE TABLE user_logs_part
(
    visitor_uuid VARCHAR(128),
    user_id      UUID REFERENCES users (user_id),
    event        VARCHAR(128),
    datetime     TIMESTAMP DEFAULT current_timestamp,
    log_date     DATE      DEFAULT current_date,
    log_id       BIGSERIAL,
    PRIMARY KEY (log_id, log_date)
) PARTITION BY RANGE (log_date);

CREATE TABLE user_logs_part_q1_y2021 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-01-01') to ('2021-04-01');
CREATE TABLE user_logs_part_q2_y2021 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-04-01') to ('2021-07-01');
CREATE TABLE user_logs_part_q3_y2021 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-07-01') to ('2021-10-01');
CREATE TABLE user_logs_part_q4_y2021 PARTITION OF user_logs_part
    FOR VALUES FROM ('2021-10-01') to ('2022-01-01');

INSERT INTO user_logs_part(visitor_uuid, user_id, event, datetime, log_date)
SELECT visitor_uuid, user_id, event, datetime, log_date
FROM user_logs;

DROP TABLE user_logs CASCADE;

-- 5 Задание                  ****************************
/*Создаем материализированое представление которое будет ежедневно обновляться в не рабочее время.
(Опиционально) Создаем индекс на поле date*/

CREATE MATERIALIZED VIEW stat_orders AS
(
SELECT o.order_dt::date                                    AS date,
       CASE
           WHEN extract(year from o.order_dt::date) - extract(year from u.birth_date::date) < 21 THEN '0-20'
           WHEN extract(year from o.order_dt::date) - extract(year from u.birth_date::date) < 31 THEN '20-30'
           WHEN extract(year from o.order_dt::date) - extract(year from u.birth_date::date) < 41 THEN '30-40'
           ELSE '40-100'
           END                                             AS age,
       (SUM(spicy) / (SUM(oi.count) / 100))::numeric(5, 2) as spicy,
       (SUM(fish) / (SUM(oi.count) / 100))::numeric(5, 2)  as fish,
       (SUM(meat) / (SUM(oi.count) / 100))::numeric(5, 2)  as meat
FROM order_items oi
         LEFT JOIN dishes d ON oi.item = d.object_id
         LEFT JOIN orders o ON o.order_id = oi.order_id
         LEFT JOIN users u ON o.user_id = u.user_id
WHERE order_dt::date < current_date
GROUP BY age, date
ORDER BY date
    );

CREATE INDEX stat_orders_date_idx ON stat_orders (date);