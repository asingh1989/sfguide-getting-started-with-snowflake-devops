USE ROLE ACCOUNTADMIN;

CREATE OR ALTER WAREHOUSE QUICKSTART_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 300 
  AUTO_RESUME= TRUE;

-- Use the warehouse immediately after creating it
USE WAREHOUSE QUICKSTART_WH;

-- Separate database for git repository
CREATE OR ALTER DATABASE QUICKSTART_COMMON;

-- API integration is needed for GitHub integratio
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/asingh1989') -- INSERT YOUR GITHUB USERNAME HERE
  ENABLED = TRUE;

-- Git repository object is similar to external stage
CREATE OR REPLACE GIT REPOSITORY quickstart_common.public.quickstart_repo
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/asingh1989/sfguide-getting-started-with-snowflake-devops'; -- INSERT URL OF FORKED REPO HERE

-- Set parameters from workflow
SET DATABASE_NAME = '{database_name}';
SET WAREHOUSE_NAME = '{warehouse_name}';

-- Create environment-specific database using parameter first
CREATE OR ALTER DATABASE IDENTIFIER($DATABASE_NAME);

-- Ensure we're using the correct warehouse and database context
USE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME);
USE DATABASE IDENTIFIER($DATABASE_NAME);

-- To monitor data pipeline's completion
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;

-- Now that we're using the correct database, create the schemas
CREATE OR ALTER SCHEMA bronze;
CREATE OR ALTER SCHEMA silver; 
CREATE OR ALTER SCHEMA gold;

-- Explicitly set context before creating schema objects
USE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME);
USE DATABASE IDENTIFIER($DATABASE_NAME);
USE SCHEMA bronze;

-- Schema level objects
CREATE OR REPLACE FILE FORMAT json_format TYPE = 'json';
CREATE OR ALTER STAGE raw;

-- Copy file from GitHub to internal stage with full context
COPY FILES INTO @raw FROM @quickstart_common.public.quickstart_repo/branches/main/data/airport_list.json;

LIST @raw;