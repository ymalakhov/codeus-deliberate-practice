-- 45. Find branches where the number of employees is at least 10% of the total number of employees.
select branch_id, count(id) employee_count
from employees
group by branch_id
having count(id) * 100 / (select count(*) from employees) > 10;