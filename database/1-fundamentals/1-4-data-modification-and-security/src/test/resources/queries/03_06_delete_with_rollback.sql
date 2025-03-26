-- check the schema for employees table in 'resources/schema.sql'
-- start transaction, delete a row from employees and rollback the transaction
-- notice that no rows were affected
BEGIN TRANSACTION;

DELETE FROM employees where last_name = 'Patron';

ROLLBACK;