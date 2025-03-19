SELECT *
FROM Transactions
WHERE transaction_date >= '2023-09-01' AND transaction_date <= '2023-09-30' AND amount > 8000
ORDER BY transaction_date ASC, amount DESC;