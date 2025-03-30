-- 27. Find the average salary per branch.
select branch_id, avg(salary) avg_salary
from employees
group by branch_id;