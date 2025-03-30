-- 12: Find the minimum and maximum account balance for checking accounts only
select min(balance) lowest_balance, max(balance) highest_balance
from accounts
where account_type = 'checking';