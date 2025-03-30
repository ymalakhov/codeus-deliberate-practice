-- 18. Count the number of employees per branch.
select branch_id, count(*) employee_count
from employees
group by branch_id;