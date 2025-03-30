-- 44. Find customers who have loans but no accounts.
select customer_id
from loans l
where (select count(*) from accounts a where a.customer_id = l.customer_id) = 0;