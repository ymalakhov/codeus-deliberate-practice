-- 21. Find the branch with the highest number of employees.
select branch_id, count(*) employee_count
from employees
group by branch_id
order by employee_count desc
limit 1;