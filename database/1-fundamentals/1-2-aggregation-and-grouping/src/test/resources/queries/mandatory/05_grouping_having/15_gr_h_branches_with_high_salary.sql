-- 15. Find branches where the average salary of employees is above $50,000.
select branch_id, avg(salary) avg_salary
from employees
group by branch_id
having avg(salary) > 50000;