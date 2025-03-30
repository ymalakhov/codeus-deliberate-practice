-- 30: Calculate the average loan term (in months) for closed loans
select avg(term_months) avg_loan_term
from loans
where status = 'closed';