-- check the schema for customers table in 'resources/schema.sql'
-- Instead of deleting all records with no conditions, use soft delete approach
-- Alter table customers and add non-nullable 'is_deleted' column with a default value
-- Update the newly created columns to true for all rows
ALTER TABLE customers
ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE customers SET is_deleted = true;