-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)

SELECT c.company_name, CONCAT(e.first_name, ' ', e.last_name) AS full_name
FROM orders AS o
INNER JOIN shippers AS s ON o.ship_via = s.shipper_id
INNER JOIN customers AS c ON c.customer_id = o.customer_id
INNER JOIN employees AS e ON e.employee_id = o.employee_id
WHERE s.company_name = 'United Package' AND c.city = 'London' AND e.city = 'London';

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.

SELECT p.product_name, p.units_in_stock, s.contact_name, s.phone FROM products AS p
INNER JOIN suppliers AS s ON p.supplier_id = s.supplier_id
INNER JOIN categories AS c ON p.category_id = c.category_id
WHERE p.discontinued = 0  AND p.units_in_stock < 25 AND (c.category_name = 'Dairy Products' OR c.category_name = 'Condiments')
ORDER BY p.units_in_stock;

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа

SELECT c.company_name FROM customers AS c
LEFT JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.

SELECT product_name FROM products
WHERE product_id IN (SELECT product_id FROM order_details WHERE quantity = 10);


