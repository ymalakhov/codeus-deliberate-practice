-- 38. Find branches where total employee salaries exceed $100,000.
select branch_id, sum(salary) total_salary
from employees
group by branch_id
having sum(salary) > 100000;
