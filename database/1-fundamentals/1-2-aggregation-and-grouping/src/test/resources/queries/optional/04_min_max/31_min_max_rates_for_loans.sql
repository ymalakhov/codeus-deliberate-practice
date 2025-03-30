-- 31: Find the minimum and maximum interest rates
--for loans with terms longer than 36 months
select min(interest_rate) lowest_rate, max(interest_rate) highest_rate
from loans
where term_months > 36;
