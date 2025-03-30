-- 17. Count the number of loans for each loan status.
select status, count(*) total_loans
from loans
group by status;
