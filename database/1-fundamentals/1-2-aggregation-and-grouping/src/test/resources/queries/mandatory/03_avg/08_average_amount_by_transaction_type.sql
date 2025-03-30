-- 8. Find the average transaction amount per transaction type.
select transaction_type, avg(amount) as avg_transaction
from transactions
group by transaction_type;