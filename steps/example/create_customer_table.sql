
CREATE or replace TABLE customer (
  id number not null, 
  first_name varchar, 
  last_name varchar
);

EXECUTE IMMEDIATE FROM 'insert_customers.sql';