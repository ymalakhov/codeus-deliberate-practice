-- 3. Find the total number of customers with at least one loan.
select count(*) as customers_with_loans from customers c
where (select count(*) from loans l where l.customer_id = c.id) >= 1;
