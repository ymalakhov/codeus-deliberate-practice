SELECT customer_name
FROM Customers
WHERE customer_id IN (SELECT DISTINCT customer_id FROM Accounts WHERE account_type = 'savings')
  AND (city = 'Kyiv' OR city = 'Lviv')
ORDER BY city ASC, customer_name ASC;