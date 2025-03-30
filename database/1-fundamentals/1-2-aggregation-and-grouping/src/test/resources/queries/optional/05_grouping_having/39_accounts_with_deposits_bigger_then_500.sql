-- 39. Find accounts that have received deposits totaling more than $500.
select account_id, sum(amount) total_deposits
from transactions
group by account_id
having sum(amount) > 500;