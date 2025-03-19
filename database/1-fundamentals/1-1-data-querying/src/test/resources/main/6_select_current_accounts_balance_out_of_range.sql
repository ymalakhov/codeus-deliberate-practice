SELECT *
FROM Accounts
WHERE account_type = 'checking' AND (balance < 1000 OR balance > 100000);