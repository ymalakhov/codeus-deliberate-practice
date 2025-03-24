--Task: Analyze Active Accounts by Type
--
--Write a query to summarize active accounts using CTEs.
--Requirements:
--- Group accounts by type (savings/checking)
--- Count active accounts for each type
--- Find most recent transaction date per type
--- Only include accounts with transactions from 2023
--- Use CTE to handle the initial account activity analysis
--- Sort results by account type
--- Consider transaction dates for activity status
--
--Expected columns: account_type, active_account_count, most_recent_transaction
WITH active_accounts AS (
    SELECT
        a.account_id,
        a.account_type,
        MAX(t.transaction_date) as last_transaction_date,
        COUNT(t.transaction_id) as transaction_count
    FROM accounts a
    LEFT JOIN transactions t ON a.account_id = t.account_id
    WHERE t.transaction_date >= '2023-01-01'
    GROUP BY a.account_id, a.account_type
)
SELECT
    account_type,
    COUNT(*) as active_account_count,
    MAX(last_transaction_date) as most_recent_transaction
FROM active_accounts
GROUP BY account_type
ORDER BY account_type;