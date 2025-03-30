-- 19. Find the number of accounts per customer.
select customer_id, count(*) account_count
from accounts
group by customer_id;