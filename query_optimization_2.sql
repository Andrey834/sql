SELECT pg_stat_statements_reset();

-- 1
-- вычисляет среднюю стоимость блюда в определенном ресторане
EXPLAIN ANALYZE
SELECT avg(dp.price)
FROM dishes_prices dp
         JOIN dishes d ON dp.dishes_id = d.object_id
WHERE d.rest_id LIKE '%14ce5c408d2142f6bd5b7afad906bc7e%'
  AND dp.date_begin::date <= current_date
  AND (dp.date_end::date >= current_date
    OR dp.date_end IS NULL);

-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
EXPLAIN ANALYZE
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
         JOIN orders o ON o.order_id = os.order_id
         JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'
  AND os.status_dt IN (SELECT max(status_dt)
                       FROM order_statuses
                       WHERE order_id = o.order_id);

--edit
/*ПРи запросе просходит соединение таблиц вложенными циклами(Nested loop).
Стоимость плана запроса: 33355
 -Оптимизируем вложенный запрос для соединения таблиц с помощью хэш-таблицы(Hash join)
Стоимость плана запроса после оптимизации: 3011
  */

EXPLAIN ANALYZE
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM orders o
         JOIN (SELECT order_id, MAX(status_id) as status_id
               FROM order_statuses
               GROUP BY order_id) os
              ON o.order_id = os.order_id
         JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c';


-- 3
-- выводит id и имена пользователей, фамилии которых входят в список
EXPLAIN ANALYZE
SELECT u.user_id, u.first_name
FROM users u
WHERE u.last_name IN
      ('КЕДРИНА', 'АДОА', 'АКСЕНОВА', 'АЙМАРДАНОВА', 'БОРЗЕНКОВА', 'ГРИПЕНКО', 'ГУЦА', 'ЯВОРЧУКА', 'ХВИЛИНА', 'ШЕЙНОГА',
       'ХАМЧИЧЕВА', 'БУХТУЕВА', 'МАЛАХОВЦЕВА', 'КРИСС', 'АЧАСОВА', 'ИЛЛАРИОНОВА', 'ЖЕЛЯБИНА', 'СВЕТОЗАРОВА', 'ИНЖИНОВА',
       'СЕРДЮКОВА', 'ДАНСКИХА')
ORDER BY 1 DESC;

-- 4
-- ищет все салаты в списке блюд
EXPLAIN ANALYZE
SELECT d.object_id, d.name
FROM dishes d
WHERE d.name LIKE 'salat%';

-- 5
-- определяет максимальную и минимальную сумму заказа по городу
EXPLAIN ANALYZE
SELECT max(p.payment_sum) max_payment, min(p.payment_sum) min_payment
FROM payments p
         JOIN orders o ON o.order_id = p.order_id
WHERE o.city_id = 2;

-- 6
-- ищет всех партнеров определенного типа в определенном городе
EXPLAIN ANALYZE
SELECT p.id partner_id, p.chain partner_name
FROM partners p
         JOIN cities c ON c.city_id = p.city_id
WHERE p.type = 'Пекарня'
  AND c.city_name = 'Владивосток';

-- 7
-- ищет действия и время действия определенного посетителя
EXPLAIN ANALYZE
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

--edit

--edit
/*План запроса показал что, партицированые таблицы последовательно сканируются.
Стоимость плана запроса: 92142
 -Добавляем индексы на необходимые поля(см. нижне) для поиска по индексу
Стоимость плана запроса после оптимизации: 45
  */
create index user_logs_visitor_uuid_idx ON user_logs (visitor_uuid)
    include (event, datetime);
create index user_logs_y2021q2_visitor_uuid_idx ON user_logs_y2021q2 (visitor_uuid)
    include (event, datetime);
create index user_logs_y2021q3_visitor_uuid_idx ON user_logs_y2021q3 (visitor_uuid)
    include (event, datetime);
create index user_logs_y2021q4_visitor_uuid_idx ON user_logs_y2021q4 (visitor_uuid)
    include (event, datetime);


-- 8
-- ищет логи за текущий день
EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE datetime::date = current_date;

--edit
/*План запроса показал что, партицированые таблицы последовательно сканируются.
Стоимость плана запроса: 104194
 -Чтобы ускорить запрос, изменяем поле сортировки на log_date. Чтобы не преобразовавать в date поле datetime
 -Добавляем индексы на необходимые поля
Стоимость плана запроса после оптимизации: 25
  */

create index user_logs_log_date_idx ON user_logs (log_date)
    include (visitor_uuid, user_id, event, datetime, log_id);
create index user_logs_y2021q2_log_date_idx ON user_logs_y2021q2 (log_date)
    include (visitor_uuid, user_id, event, datetime, log_id);
create index user_logs_y2021q3_log_date_idx ON user_logs_y2021q3 (log_date)
    include (visitor_uuid, user_id, event, datetime, log_id);
create index user_logs_y2021q4_log_date_idx ON user_logs_y2021q4 (log_date)
    include (visitor_uuid, user_id, event, datetime, log_id);

EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE log_date = current_date;

-- 9
-- определяет количество неоплаченных заказов
EXPLAIN ANALYZE
SELECT o.order_id
FROM order_statuses os
         JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
       FROM order_statuses os1
       WHERE os1.order_id = o.order_id
         AND os1.status_id = 2) = 0
  AND o.city_id = 1;

--edit

/*План запроса показал что, происходит соединение таблиц вложенными циклами(Nested Lopp).
Так же отсутствуют индексы для задействованых полей в запросе.
Стоимость плана запроса: 61289503
 -Чтобы ускорить запрос, добавляем индексы для двух таблиц(см. ниже)
 -Корректируем запрос: Убираем агрегатную функцию из подзапроса
                       Оставляем только необходимые поля
Стоимость плана запроса после оптимизации: 3941*/

create index order_statuses_status_id_idx ON order_statuses (status_id, order_id);
create index orders_city_id_idx ON orders (city_id);

EXPLAIN ANALYZE
SELECT count(os.order_id)
FROM order_statuses os
         JOIN orders o ON o.order_id = os.order_id
WHERE os.order_id NOT IN (SELECT order_id
                          FROM order_statuses
                          WHERE status_id = 2)
  AND o.city_id = 1;
-------------------------------------------------------------------------

-- 10
-- определяет долю блюд дороже 1000
EXPLAIN ANALYZE
SELECT (SELECT count(*)
        FROM dishes_prices dp
        WHERE dp.date_end IS NULL
          AND dp.price > 1000.00)::NUMERIC / count(*)::NUMERIC
FROM dishes_prices
WHERE date_end IS NULL;

-- 11
-- отбирает пользователей определенного города, чей день рождения находится в интервале +- 3 дня от текущей даты
EXPLAIN ANALYZE
SELECT user_id, current_date - birth_date
FROM users
WHERE city_id = 1
  AND birth_date >= current_date - 3
  AND birth_date <= current_date + 3;

-- 12
-- вычисляет среднюю стоимость блюд разных категорий
EXPLAIN ANALYZE
SELECT 'average price with fish', avg(dp.price)
FROM dishes_prices dp
         JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL
  AND d.fish = 1
UNION
SELECT 'average price with meat', avg(dp.price)
FROM dishes_prices dp
         JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL
  AND d.meat = 1
UNION
SELECT 'average price of spicy food', avg(dp.price)
FROM dishes_prices dp
         JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL
  AND d.spicy = 1
ORDER BY 2;

-- 13
-- ранжирует города по общим продажам за определенный период
EXPLAIN ANALYZE
SELECT ROW_NUMBER() OVER ( ORDER BY sum(o.final_cost) DESC),
       c.city_name,
       sum(o.final_cost)
FROM cities c
         JOIN orders o ON o.city_id = c.city_id
WHERE order_dt >= to_timestamp('01.01.2021 00-00-00', 'dd.mm.yyyy hh24-mi-ss')
  AND order_dt < to_timestamp('02.01.2021', 'dd.mm.yyyy hh24-mi-ss')
GROUP BY c.city_name;

-- 14
-- вычисляет количество заказов определенного пользователя
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM orders
WHERE user_id = '0fd37c93-5931-4754-a33b-464890c22689';

-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего
EXPLAIN ANALYZE
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
         JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (SELECT item
                  FROM (SELECT item, SUM(count) AS total_sales
                        FROM order_items oi
                        GROUP BY 1) dishes_sales
                  WHERE dishes_sales.total_sales > (SELECT SUM(t.total_sales) / COUNT(*)
                                                    FROM (SELECT item, SUM(count) AS total_sales
                                                          FROM order_items oi
                                                          GROUP BY 1) t))
GROUP BY 1
ORDER BY orders_quantity DESC;

--edit
/*Исходя из плана запроса, делаем вывод что проблема заключается в агрегатных функциях в подзапросах
Стоимость плана запроса: 4810
 -Чтобы ускорить запрос, добавляем индекс к таблице order_items поле item. Так же включаем поле count.
 -Создаем CTE для использования в подзапросах
 -С учетом специфики запроса, а именно сравнение каждой строки с средним значением. Используем Nested loop
Стоимость плана запроса после оптимизации: 2822*/

CREATE INDEX order_items_item_idx ON order_items (item)
    include (count);

EXPLAIN ANALYZE
WITH orders_sum AS (SELECT item, SUM(count) AS total_sales
                    FROM order_items oi
                    GROUP BY 1)
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
         JOIN dishes d ON d.object_id = oi.item
         JOIN orders_sum os ON os.item = oi.item AND os.total_sales > (SELECT SUM(total_sales) / COUNT(*)
                                                                       FROM orders_sum)
GROUP BY d.name
ORDER BY orders_quantity DESC;