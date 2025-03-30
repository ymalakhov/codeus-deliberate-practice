-- 36. Find the number of customers with more than one account.
select customer_id, count(*) account_count
from accounts
group by customer_id
having count(id) > 1;