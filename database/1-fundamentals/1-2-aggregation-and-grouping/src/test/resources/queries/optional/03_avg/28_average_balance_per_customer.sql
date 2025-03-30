-- 28. Find the average balance per customer.
select customer_id, avg(balance) avg_balance
from accounts
group by customer_id;