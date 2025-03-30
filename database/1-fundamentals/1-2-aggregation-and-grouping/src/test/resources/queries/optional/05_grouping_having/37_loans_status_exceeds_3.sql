-- 37. Find loan statuses where the total number of loans exceeds 3.
select status, count(*) loan_count
from loans
group by status
having count(*) > 3;