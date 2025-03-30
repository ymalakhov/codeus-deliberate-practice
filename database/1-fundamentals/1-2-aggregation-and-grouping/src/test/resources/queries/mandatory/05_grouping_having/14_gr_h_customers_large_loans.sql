-- 14. Find customers who have taken loans totaling more than $15,000.
select customer_id, sum(amount) total_loan
from loans
group by customer_id
having sum(amount) > 15000;