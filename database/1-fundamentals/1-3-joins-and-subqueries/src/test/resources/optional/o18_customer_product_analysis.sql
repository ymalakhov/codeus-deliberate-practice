--Task: Comprehensive Customer Value Analysis
--
--Write a query to analyze customer value using multiple CTEs and weighted scoring.
--Requirements:
--- Use two CTEs: transaction_metrics and loan_metrics
--- In transaction_metrics CTE:
    -- Calculate total number of transactions
    -- Sum deposits and withdrawals separately
    -- Calculate average balance across all accounts
--- In loan_metrics CTE:
    -- Count total loans per customer
    -- Calculate total loan amount
    -- Calculate average interest rate
--- Main query should:
    -- Display customer's first and last name
    -- Show all metrics from both CTEs
    -- Calculate customer value score using weighted formula:
--       * 40% weight for average balance
--       * 30% weight for total deposits
--       * 30% weight for total loan amount
    -- Round all monetary values to 2 decimal places
    -- Divide final score by 1000 for readability
--
--Expected columns:
--- first_name
--- last_name
--- total_transactions
--- total_deposits
--- total_withdrawals
--- avg_balance
--- loan_count
--- total_loan_amount
--- customer_value_score (rounded to 2 decimal places)
--
--Note: All NULL values should be converted to 0 in calculations using COALESCE
--Sort by customer_value_score in descending order
