-- 9. Find the average interest rate per loan status.
select status, avg(interest_rate) as avg_interest
from loans
group by status;