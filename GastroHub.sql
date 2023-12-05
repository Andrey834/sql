/*1 Этап*/

--Создание ENUM

CREATE TYPE cafe.restaurant_type AS ENUM
    ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

--Создание таблиц

CREATE TABLE cafe.restaurants
(
    restaurant_uuid     uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    restaurant_name     varchar(50) UNIQUE,
    restaurant_location geometry(POINT),
    restaurant_type     cafe.restaurant_type,
    restaurant_menu     jsonb
);

CREATE TABLE cafe.managers
(
    manager_uuid  uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    manager_name  VARCHAR(50),
    manager_phone VARCHAR(50) UNIQUE
);

CREATE TABLE cafe.restaurant_manager_work_dates
(
    restaurant_uuid uuid references cafe.restaurants (restaurant_uuid),
    manager_uuid    uuid references cafe.managers (manager_uuid),
    start_date      date,
    end_date        date,
    experience      VARCHAR(23),
    PRIMARY KEY (restaurant_uuid, manager_uuid)
);

CREATE TABLE cafe.sales
(
    date            date,
    restaurant_uuid uuid references cafe.restaurants (restaurant_uuid),
    avg_check       numeric(6, 2),
    PRIMARY KEY (date, restaurant_uuid)
);

--Заполнение таблиц

INSERT INTO cafe.restaurants(restaurant_name, restaurant_location, restaurant_type, restaurant_menu)
SELECT DISTINCT sl.cafe_name,
                ST_SetSRID(st_point(sl.longitude, sl.latitude), 4326),
                sl.type::cafe.restaurant_type,
                mn.menu::jsonb
FROM raw_data.sales sl,
     raw_data.menu mn
WHERE sl.cafe_name = mn.cafe_name;

INSERT INTO cafe.managers(manager_name, manager_phone)
SELECT DISTINCT mn.manager,
                '+7' ||
                substr((select regexp_replace(mn.manager_phone, '[^0-9]+', '', 'g')), 2, length(mn.manager_phone))
FROM raw_data.sales mn;

INSERT INTO cafe.restaurant_manager_work_dates(restaurant_uuid, manager_uuid, start_date, end_date, experience)
SELECT rs.restaurant_uuid,
       mn.manager_uuid,
       MIN(sl.report_date),
       MAX(sl.report_date),
       (MAX(sl.report_date)::date - MIN(sl.report_date)::date)
FROM raw_data.sales sl,
     cafe.restaurants rs,
     cafe.managers mn
WHERE sl.cafe_name = rs.restaurant_name
  AND sl.manager = mn.manager_name
GROUP BY rs.restaurant_uuid, mn.manager_uuid;

INSERT INTO cafe.sales(date, restaurant_uuid, avg_check)
SELECT sl.report_date, rs.restaurant_uuid, sl.avg_check
FROM raw_data.sales sl,
     cafe.restaurants rs
WHERE sl.cafe_name = rs.restaurant_name;

/*2 Этап*/

--1 Задание

WITH sales_top AS (SELECT rs.restaurant_name                                                         AS name,
                          rs.restaurant_type                                                         AS type,
                          AVG(avg_check)::numeric(6, 2)                                              AS avg_check,
                          RANK() OVER (PARTITION BY rs.restaurant_type ORDER BY AVG(avg_check) DESC) AS rank
                   FROM cafe.sales sl,
                        cafe.restaurants rs
                   WHERE sl.restaurant_uuid = rs.restaurant_uuid
                   GROUP BY type, name
                   ORDER BY avg_check DESC)

SELECT name, type, avg_check
FROM sales_top
WHERE rank <= 3;

--2 Задание

CREATE MATERIALIZED VIEW cafe.v_dynamic_sales AS
SELECT EXTRACT(YEAR FROM s.date)             AS year,
       r.restaurant_name                     AS name,
       r.restaurant_type                     AS type,
       AVG(s.avg_check)::numeric(6, 2)       AS avg_current,
       LAG(AVG(s.avg_check)::numeric(6, 2))
       OVER (PARTITION BY r.restaurant_name) AS avg_previous,
       ((((AVG(s.avg_check)::numeric(6, 2)) /
          LAG(AVG(s.avg_check)::numeric(6, 2)) OVER (PARTITION BY r.restaurant_name)) - 1) *
        100)::numeric(5, 2)                  AS diff_to_prev_year
FROM cafe.sales s,
     cafe.restaurants r
WHERE s.restaurant_uuid = r.restaurant_uuid
  AND EXTRACT(YEAR FROM s.date) <> 2023
GROUP BY year, name, type
ORDER BY name, year;

SELECT *
FROM cafe.v_dynamic_sales;

--3 Задание

SELECT r.restaurant_name               AS name,
       COUNT(DISTINCT rm.manager_uuid) AS count
FROM cafe.restaurant_manager_work_dates rm,
     cafe.restaurants r
WHERE rm.restaurant_uuid = r.restaurant_uuid
GROUP BY restaurant_name
ORDER BY count DESC, name
LIMIT 3;

--4 Задание

WITH top AS (SELECT r.name, count(r.menu) AS count, dense_rank() OVER (ORDER BY count(r.menu) DESC) AS place
             FROM (SELECT r.restaurant_name                               AS name,
                          jsonb_object_keys(r.restaurant_menu -> 'Пицца') AS menu
                   FROM cafe.restaurants r
                   WHERE r.restaurant_type = 'pizzeria') AS r
             GROUP BY name)

SELECT name, count
FROM top
WHERE place = 1;

--5 Задание

WITH menu_cte AS (SELECT r.restaurant_name                                        AS name,
                         'Пицца'                                                  AS type,
                         jsonb_object_keys(r.restaurant_menu -> 'Пицца')          as pizza_name,
                         ((r.restaurant_menu -> 'Пицца') ->>
                          (jsonb_object_keys(r.restaurant_menu -> 'Пицца')))::int as price
                  FROM cafe.restaurants r
                  WHERE r.restaurant_type = 'pizzeria'),

     menu_with_rank AS (SELECT *, row_number() OVER (PARTITION BY mc.name ORDER BY mc.price DESC) AS place
                        FROM menu_cte mc
                        ORDER BY price DESC)

SELECT name, type, pizza_name, price
FROM menu_with_rank
WHERE place = 1;

--6 Задание

SELECT r.restaurant_name                                                                AS name1,
       rs.restaurant_name                                                               AS name2,
       r.restaurant_type                                                                AS type,
       ST_Distance(r.restaurant_location::geography, rs.restaurant_location::geography) as distance
FROM cafe.restaurants r
         CROSS JOIN cafe.restaurants rs
WHERE r.restaurant_type = rs.restaurant_type
  AND r.restaurant_name != rs.restaurant_name
order by distance
limit 1;

--7 Задание

WITH location AS (SELECT d.district_name as name, count(r.restaurant_uuid) as count
                  FROM cafe.districts d,
                       cafe.restaurants r
                  WHERE st_within(r.restaurant_location, d.district_geom)
                  GROUP BY name)

    (SELECT name, count
     FROM location
     ORDER BY count DESC
     LIMIT 1)
UNION
(SELECT name, count
 FROM location
 ORDER BY count
 LIMIT 1)
ORDER BY count DESC;