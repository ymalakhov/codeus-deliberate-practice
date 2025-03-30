-- 40. Find the average balance of customers who have more than one account.
select customer_id, avg(balance) avg_balance
from accounts
group by customer_id
having count(id) > 1;