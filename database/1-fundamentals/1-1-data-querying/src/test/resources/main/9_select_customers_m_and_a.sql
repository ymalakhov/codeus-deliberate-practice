SELECT *
FROM Customers
WHERE LOWER(customer_name) LIKE 'm%' AND LOWER(customer_name) LIKE '%a%';