-- 20. Find the number of transactions per account.
select account_id, count(*) transaction_count
from transactions
group by account_id;