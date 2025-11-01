-- Create file "create_customer_table.sql" and commit it to your git repo
-- The file should have the following contents:
#!jinja
CREATE TABLE customer (
  id number not null, 
  first_name varchar, 
  last_name varchar
);
GRANT OWNERSHIP ON TABLE customer TO ROLE {{owner}};
EXECUTE IMMEDIATE FROM 'insert_customers.sql';