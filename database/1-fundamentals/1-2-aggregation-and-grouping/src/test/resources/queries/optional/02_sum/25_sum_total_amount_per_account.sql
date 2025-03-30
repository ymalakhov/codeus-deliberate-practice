-- 25. Find the total amount transacted per account.
select account_id, sum(amount) total_amount
from transactions
group by account_id;