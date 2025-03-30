-- 24. Find the total balance for each customer.
select customer_id, sum(balance) total_balance
from accounts
group by customer_id;
