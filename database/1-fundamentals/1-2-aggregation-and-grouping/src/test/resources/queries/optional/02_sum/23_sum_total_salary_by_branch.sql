-- 23. Find the total salary expenditure per branch.
select branch_id, sum(salary) total_salary
from employees
group by branch_id;