-- 22. Find the total amount of loans per loan status.
select status, sum(amount) total_loan_amount
from loans
group by status;