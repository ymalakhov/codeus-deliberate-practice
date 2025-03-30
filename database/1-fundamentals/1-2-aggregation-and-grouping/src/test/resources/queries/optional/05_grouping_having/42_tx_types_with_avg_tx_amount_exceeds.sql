-- 42. Find transaction types where the average transaction amount is greater than $250.
select transaction_type, avg(amount) avg_tx_amount
from transactions
group by transaction_type
having avg(amount) > 250;