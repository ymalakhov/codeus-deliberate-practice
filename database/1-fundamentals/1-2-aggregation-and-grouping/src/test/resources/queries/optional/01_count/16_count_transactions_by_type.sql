-- 16. Count the number of transactions per transaction type.
select transaction_type, count(*) transaction_count
from transactions
group by transaction_type;