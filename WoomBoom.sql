CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.sales
(
    id                   INT4,
    auto                 VARCHAR(30),
    gasoline_consumption NUMERIC(3, 1),
    price                NUMERIC(9, 2),
    date                 DATE,
    person_name          VARCHAR(150),
    phone                VARCHAR(25),
    discount             INT2,
    brand_origin         VARCHAR(50)
);

COPY raw_data.sales (id,
                     auto,
                     gasoline_consumption,
                     price,
                     date,
                     person_name,
                     phone,
                     discount,
                     brand_origin)
    --FROM '/usr/share/cars.csv'
    FROM '/home/andrey834/sambashare/sql/cars.csv'
    WITH CSV HEADER NULL 'null';

--new schema
CREATE SCHEMA IF NOT EXISTS car_shop;

--new tables
CREATE TABLE IF NOT EXISTS car_shop.clients
(
    id         SERIAL4 PRIMARY KEY, --2147483647 для клиентов достаточно, учитывая что за год добавили 894
    first_name VARCHAR(50),         --в РФ самое длинное имя 15 символов, делаем с запасом в 50 символов
    last_name  VARCHAR(100),        --решил подстраховаться, на случай двойных фамилий
    phone      VARCHAR(25),         --22 символа макс. длинна входных данных в столбце -phone, плюс запас 3 символа
    CONSTRAINT clients_unique UNIQUE (first_name, last_name, phone)
);

INSERT INTO car_shop.clients(first_name, last_name, phone)
SELECT INITCAP(split_part(sales.person_name, ' ', 1)),
       INITCAP(split_part(sales.person_name, ' ', -1)),
       sales.phone
FROM raw_data.sales
ORDER BY sales.person_name
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS car_shop.color
(
    id    SERIAL2 PRIMARY KEY, --предположил что количество цветов будет не больше 32767, поэтому выбрал smallserial
    color VARCHAR(30) UNIQUE   --загуглил University of Pennsylvania red (30 characters long)
);

INSERT INTO car_shop.color(color)
SELECT DISTINCT LOWER(split_part(sales.auto, ' ', -1)) as color
FROM raw_data.sales
ORDER BY color;


CREATE TABLE IF NOT EXISTS car_shop.country
(
    id   SERIAL2 PRIMARY KEY, --предположил что количество стран будет не больше 32767, поэтому выбрал smallserial
    name VARCHAR(32) UNIQUE   --Saint Vincent and the Grenadines самое длинное название страны. 32 символа
);

INSERT INTO car_shop.country(name)
SELECT DISTINCT UPPER(TRIM(sales.brand_origin)) as country
FROM raw_data.sales
WHERE sales.brand_origin IS NOT NULL
ORDER BY country;


CREATE TABLE IF NOT EXISTS car_shop.brand
(
    id      SERIAL2 PRIMARY KEY, --предположил что количество брэндов будет не больше 32767, поэтому выбрал smallserial
    name    VARCHAR(50) UNIQUE,  --не существует названий брендов длиной более 50 символов, в названии бренда могут быть и цифры, и буквы
    country INT2 REFERENCES car_shop.country (id)
);

INSERT INTO car_shop.brand(name, country)
SELECT DISTINCT UPPER(split_part(sales.auto, ' ', 1)) as brand,
                (SELECT id
                 FROM car_shop.country
                 WHERE name = UPPER(sales.brand_origin))
FROM raw_data.sales
ORDER BY brand;


CREATE TABLE IF NOT EXISTS car_shop.cars
(
    id                   SERIAL4 PRIMARY KEY,
    name                 VARCHAR(30)                         NOT NULL, /*не существует названий автомобилей длиной более
                             30 символов, в названии бренда могут быть цифры и буквы, поэтому выбираем varchar(30)*/
    brand                INT2 REFERENCES car_shop.brand (id) NOT NULL,
    gasoline_consumption FLOAT(2)
);

INSERT INTO car_shop.cars(name, brand, gasoline_consumption)
SELECT DISTINCT TRIM(SPLIT_PART(SPLIT_PART(sales.auto, ',', 1), SPLIT_PART(sales.auto, ' ', 1), - 1)) as name,
                (SELECT id
                 FROM car_shop.brand
                 WHERE name = UPPER(SPLIT_PART(sales.auto, ' ', 1)))                                  as brand,
                sales.gasoline_consumption                                                            as gasoline
FROM raw_data.sales
GROUP BY brand, name, gasoline
ORDER BY brand;



CREATE TABLE IF NOT EXISTS car_shop.invoice
(
    id       SERIAL8 PRIMARY KEY,
    car      INT4 REFERENCES car_shop.cars (id)    NOT NULL,
    color    INT2 REFERENCES car_shop.color (id)   NOT NULL,
    client   INT4 REFERENCES car_shop.clients (id) NOT NULL,
    price    NUMERIC(9, 2),
    discount INT2 DEFAULT 0,
    date     DATE DEFAULT CURRENT_DATE
);

INSERT INTO car_shop.invoice(car, color, client, price, discount, date)
SELECT (SELECT id
        FROM car_shop.cars
        WHERE name = TRIM(SPLIT_PART(SPLIT_PART(raw_data.sales.auto, ',', 1), SPLIT_PART(sales.auto, ' ', 1), - 1))
          AND brand = (SELECT id
                       FROM car_shop.brand
                       WHERE name = UPPER(SPLIT_PART(raw_data.sales.auto, ' ', 1)))),
       (SELECT id
        FROM car_shop.color
        WHERE color.color = LOWER(SPLIT_PART(raw_data.sales.auto, ' ', -1))),
       (SELECT id
        FROM car_shop.clients
        WHERE first_name = INITCAP(SPLIT_PART(raw_data.sales.person_name, ' ', 1))
          AND last_name = INITCAP(SPLIT_PART(raw_data.sales.person_name, ' ', -1))),
       raw_data.sales.price::NUMERIC(9, 2),
       raw_data.sales.discount,
       raw_data.sales.date::DATE as date
FROM raw_data.sales
ORDER BY date;

--Задание 1
SELECT (COUNT(DISTINCT cars.name)::REAL / 100) * (SELECT COUNT(DISTINCT name)
                                                  FROM car_shop.cars
                                                  WHERE gasoline_consumption IS NULL) AS nulls_percentage_gasoline_consumption
FROM car_shop.cars;

--Задание 2
SELECT b.name                          AS brand_name,
       EXTRACT(YEAR FROM i.date::date) AS year,
       AVG(i.price)::NUMERIC(9, 2)     AS price_avg
FROM car_shop.cars AS c
         LEFT JOIN car_shop.invoice AS i ON c.id = i.car
         LEFT JOIN car_shop.brand AS b ON c.brand = b.id
GROUP BY b.name, year
ORDER BY brand_name, year;

--Задание 3
SELECT EXTRACT(MONTH FROM i.date::date) AS month,
       EXTRACT(YEAR FROM i.date::date)  AS year,
       AVG(i.price)::numeric(9, 2)      AS price_avg
FROM car_shop.invoice i
WHERE EXTRACT(YEAR FROM i.date::date) = 2022
GROUP BY year, month
ORDER BY month;

--Задание 4
SELECT CONCAT_WS(' ', cl.first_name, cl.last_name)                      AS person,
       STRING_AGG(CONCAT_WS(' ', b.name, c.name), ', ' ORDER BY b.name) AS cars
FROM car_shop.invoice i,
     car_shop.clients cl,
     car_shop.cars c,
     car_shop.brand b
WHERE i.client = cl.id
  AND i.car = c.id
  AND c.brand = b.id
GROUP BY person
ORDER BY person;

--Задание 5
SELECT co.name                                                      AS brand_origin,
       MAX(i.price * 100 / (100 - i.discount / 100))::NUMERIC(9, 2) AS price_max,
       MIN(i.price * 100 / (100 - i.discount / 100))::NUMERIC(9, 2) AS price_min
FROM car_shop.invoice i,
     car_shop.cars c,
     car_shop.brand b,
     car_shop.country co
WHERE i.car = c.id
  AND c.brand = b.id
  AND b.country = co.id
GROUP BY brand_origin
ORDER BY brand_origin;

--Задание 6
SELECT COUNT(*) AS persons_from_usa_count
FROM car_shop.clients
WHERE clients.phone LIKE '+1%';