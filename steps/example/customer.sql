-- Create file "customer.sql" and commit it to your git repo
-- The file should have the following contents:
--- added primary key and country_codef
CREATE OR ALTER TABLE customer (
  id number primary key, 
  first_name varchar, 
  last_name varchar,
  country_code varchar
);