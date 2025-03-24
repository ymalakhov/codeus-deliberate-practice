--Task: Analyze Employee Loan Processing Performance
--
--Create a query to evaluate employee performance based on loan processing.
--Requirements:
--- Show employee's first and last name
--- Calculate total number of loans processed by each employee
--- Assign performance ranking based on number of loans processed
--- Only include employees who have processed at least one loan
--- Rank should be assigned based on loan count (highest count gets rank 1)
--- Use appropriate window function for ranking
--
--Expected columns: first_name, last_name, loans_processed, performance_rank
SELECT
    e.first_name,
    e.last_name,
    COUNT(l.loan_id) as loans_processed,
    RANK() OVER (ORDER BY COUNT(l.loan_id) DESC) as performance_rank
FROM employees e
LEFT JOIN loans l ON e.branch_id = l.branch_id
GROUP BY e.employee_id, e.first_name, e.last_name
HAVING COUNT(l.loan_id) > 0;