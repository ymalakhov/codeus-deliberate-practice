-- 32: Find the lowest and highest loan amounts for active loans only
SELECT MIN(amount) AS smallest_loan,
       MAX(amount) AS largest_loan
FROM loans
WHERE status = 'active' AND customer_id BETWEEN 5 AND 10;