-- 34: Find the minimum and maximum balances
--for savings accounts with balances over $5,000
select min(balance) min_savings, max(balance) max_savings
from accounts
where account_type = 'savings' and balance > 5000;
