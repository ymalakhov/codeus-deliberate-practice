-- 29. Finds the average balance for accounts that have more than 2000 in them.
select avg(balance) avg_balance
from accounts
where balance > 2000;