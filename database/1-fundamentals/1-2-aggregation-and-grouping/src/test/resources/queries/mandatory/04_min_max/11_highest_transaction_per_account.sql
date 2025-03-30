-- 11. Find the highest transaction amount per account.
select account_id, max(amount) max_transaction
from transactions
group by account_id;