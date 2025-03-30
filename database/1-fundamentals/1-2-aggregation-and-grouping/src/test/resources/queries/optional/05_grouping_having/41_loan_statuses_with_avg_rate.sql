-- 41. Find loan statuses where the average interest rate is above 5%.
select status, avg(interest_rate) avg_interest_rate
from loans
group by status
having avg(interest_rate) > 5;