-- 33: Find the smallest and largest deposit transactions
select min(amount) smallest_deposit, max(amount) largest_deposit
from transactions
where transaction_type = 'deposit';