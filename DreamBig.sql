--1 Задание
CREATE OR REPLACE PROCEDURE update_employees_rate(in p_data json)
    LANGUAGE plpgsql
AS
$$
BEGIN
    FOR _i IN 1..(JSON_ARRAY_LENGTH(p_data))
        LOOP
            UPDATE employees
            SET rate = GREATEST((rate * (1 + (p_data -> _i ->> 'rate_change')::numeric / 100)), 500)
            WHERE id = (p_data -> _i ->> 'employee_id')::uuid;
        END LOOP;
END;
$$;

--Проверка 1 Задания
CALL update_employees_rate(
        '[
          {
            "employee_id": "80718590-e2bf-492b-8c83-6f8c11d007b1",
            "rate_change": 10
          },
          {
            "employee_id": "80718590-e2bf-492b-8c83-6f8c11d007b1",
            "rate_change": -5
          }
        ]'::json
     );


--2 Задание
CREATE OR REPLACE PROCEDURE indexing_salary(in p_bid int)
    LANGUAGE plpgsql
AS
$$
DECLARE
    _avg_salary numeric(7, 2);
BEGIN
    _avg_salary := (SELECT AVG(rate)::numeric FROM employees);

    UPDATE employees
    SET rate = CASE
                   WHEN rate < _avg_salary THEN round(((p_bid + 2)::numeric / 100 + 1) * rate)
                   ELSE round((p_bid::numeric / 100 + 1) * rate)
        END;
END;
$$;

--Проверка 2 Задания
CALL indexing_salary(10);


--3 Задание
CREATE OR REPLACE PROCEDURE close_project(in p_uuid uuid)
    LANGUAGE plpgsql
AS
$$
DECLARE
    _limit_hours     int;
    _spent_hours     int;
    _count_employees int;
    _bonus_hours     int;
BEGIN
    _limit_hours := (SELECT estimated_time FROM projects WHERE id = p_uuid);
    _spent_hours := (SELECT SUM(work_hours) FROM logs WHERE project_id = p_uuid);

    IF (SELECT is_active FROM projects WHERE id = p_uuid) = false THEN
        RAISE NOTICE 'Project closed';
    ELSEIF _limit_hours IS NULL OR _limit_hours <= _spent_hours THEN
        UPDATE projects
        SET is_active = false
        WHERE id = p_uuid;
    ELSE
        _count_employees := (SELECT count(DISTINCT employee_id) FROM logs WHERE project_id = p_uuid);
        _bonus_hours := LEAST(floor(((_limit_hours - _spent_hours)::numeric * 0.75) / _count_employees), 16);

        WITH employee AS (SELECT DISTINCT employee_id FROM logs WHERE project_id = p_uuid)
        INSERT
        INTO logs(employee_id, project_id, work_date, work_hours)
        SELECT employee_id, p_uuid, current_date, _bonus_hours
        FROM employee;

        UPDATE projects
        SET is_active = false
        WHERE id = p_uuid;
    END IF;
END;
$$;

--Проверка 3 Задания
CALL close_project('4abb5b99-3889-4c20-a575-e65886f266f9');


--4 Задание
CREATE OR REPLACE PROCEDURE log_work(in p_emp_uuid uuid, in p_pr_uuid uuid, in p_date date, in p_hours int)
    LANGUAGE plpgsql
AS
$$
DECLARE
    _review_mark bool;
BEGIN
    IF (SELECT is_active FROM projects WHERE id = p_pr_uuid) = false THEN
        RAISE NOTICE 'Project closed';
        RETURN;
    ELSEIF p_hours NOT BETWEEN 1 AND 24 THEN
        RAISE NOTICE 'Valid hour value: 1-24';
        RETURN;
    ELSE
        IF p_hours > 16 OR p_date > current_date OR p_date < (current_date - 7) THEN
            _review_mark := true;
        end if;

        INSERT INTO logs(employee_id, project_id, work_date, work_hours, required_review)
        VALUES (p_emp_uuid, p_pr_uuid, p_date, p_hours, _review_mark);
    END IF;
END;
$$;

--Проверка 4 Задания
CALL log_work(
        '6db4f4a3-239b-4085-a3f9-d1736040b38c',
        '35647af3-2aac-45a0-8d76-94bc250598c2',
        '2023-10-22',
        4
     );


--5 Задание
CREATE TABLE IF NOT EXISTS employee_rate_history
(
    id          SERIAL8 PRIMARY KEY,
    employee_id UUID references employees (id),
    rate        INT CHECK ( rate > 0 ),
    from_date   DATE
);

INSERT INTO employee_rate_history(employee_id, rate, from_date)
SELECT id, rate, '2020-12-26'
FROM employees;

CREATE OR REPLACE FUNCTION save_employee_rate_history()
    RETURNS trigger
    LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO employee_rate_history(employee_id, rate, from_date)
    VALUES (NEW.id, NEW.rate, current_date);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER change_employee_rate
    AFTER INSERT OR UPDATE
    ON employees
    for each row
EXECUTE FUNCTION save_employee_rate_history();


--6 Задание
CREATE OR REPLACE FUNCTION best_project_workers(in p_pr_uuid uuid)
    RETURNS TABLE
            (
                employee   text,
                work_hours int
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT emp.employee, emp.work_hours::int
        FROM (SELECT e.name AS employee, SUM(l.work_hours) AS work_hours
              FROM employees e,
                   logs l
              WHERE e.id = l.employee_id
                AND l.project_id = p_pr_uuid
              GROUP BY e.name
              ORDER BY work_hours DESC
              LIMIT 3) as emp;
END;
$$;

--Проверка 6 Задания
SELECT employee, work_hours
FROM best_project_workers(
        '4abb5b99-3889-4c20-a575-e65886f266f9' -- Project UUID
     );


--7 Задание
CREATE OR REPLACE FUNCTION calculate_month_salary(in p_start_month date, in p_end_month date)
    RETURNS TABLE
            (
                id           uuid,
                employee     text,
                worked_hours int,
                salary       int
            )
    LANGUAGE plpgsql
AS
$$
DECLARE
    _emp record;
BEGIN
    FOR _emp IN (SELECT DISTINCT name
                 FROM employees e,
                      logs l
                 WHERE e.id = l.employee_id
                   AND l.work_date BETWEEN p_start_month AND p_end_month
                   AND l.required_review = true)
        LOOP
            RAISE NOTICE 'Warning! Employee % hours must be reviewed!', _emp.name;
        END LOOP;

    RETURN QUERY
        SELECT e.id,
               e.name,
               (SUM(l.work_hours))::int as work_hours,
               (ROUND(CASE
                          WHEN SUM(l.work_hours) > 160 THEN (SUM(l.work_hours) * e.rate) * 1.25
                          ELSE (SUM(l.work_hours) * e.rate)
                   END))::int           as salary
        FROM employees e,
             logs l
        WHERE e.id = l.employee_id
          AND l.work_date BETWEEN p_start_month AND p_end_month
          AND l.required_review = false
          AND l.is_paid = false
        GROUP BY e.id, e.name
        ORDER BY e.name;
end;
$$;

--Проверка 7 Задания
SELECT *
FROM calculate_month_salary(
        '2023-10-01', -- start of month
        '2023-10-31' -- end of month
     );