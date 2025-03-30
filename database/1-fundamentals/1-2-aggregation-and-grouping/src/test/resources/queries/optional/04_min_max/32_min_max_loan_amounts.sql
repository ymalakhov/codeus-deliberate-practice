-- 32: Find the lowest and highest loan amounts for
--active loans with customer IDs in the range of 5 to 10
select min(amount) smallest_loan, max(amount) largest_loan
from loans
where status = 'active' and customer_id between 5 and 10;