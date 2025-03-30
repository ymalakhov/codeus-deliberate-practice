-- 6. Find the top 3 customers with the highest total balance.
select customer_id, sum(balance) as total_balance
from accounts
group by customer_id
order by total_balance desc
limit 3;