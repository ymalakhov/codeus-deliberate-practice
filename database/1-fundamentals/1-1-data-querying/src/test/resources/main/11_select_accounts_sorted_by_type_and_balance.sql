SELECT *
FROM Accounts
ORDER BY
    CASE account_type
        WHEN 'savings' THEN 1
        WHEN 'checking' THEN 2
        ELSE 3
    END ASC,
    balance DESC;