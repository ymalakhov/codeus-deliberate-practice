-- check the schema for customers table in 'resources/schema.sql'
-- check the test-data for customers table in 'resources/test-data.sql'
-- update the address to 'Sumy' for non-existed id and notice it is executed without failure
UPDATE customers SET address = 'Sumy' WHERE id = 456445656;