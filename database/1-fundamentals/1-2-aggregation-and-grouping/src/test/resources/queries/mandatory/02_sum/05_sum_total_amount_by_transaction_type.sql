-- 5. Find the total amount transacted for each transaction type.
select transaction_type, sum(amount) as total_amount
from transactions
group by transaction_type;