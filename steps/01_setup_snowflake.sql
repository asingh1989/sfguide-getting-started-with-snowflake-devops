USE ROLE ACCOUNTADMIN;

CREATE OR ALTER WAREHOUSE QUICKSTART_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 300 
  AUTO_RESUME= TRUE;

-- Use the warehouse immediately after creating it
---USE WAREHOUSE QUICKSTART_WH;

-- Separate database for git repository
CREATE OR ALTER DATABASE QUICKSTART_COMMON;


-- API integration is needed for GitHub integration

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/asingh1989') -- INSERT YOUR GITHUB USERNAME HERE
  ENABLED = TRUE;

-- Git repository object is similar to external stag
CREATE OR REPLACE GIT REPOSITORY quickstart_common.public.quickstart_repo
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/asingh1989/sfguide-getting-started-with-snowflake-devops'; -- INSERT URL OF FORKED REPO HERE



-- Set parameters from workflow


-- Create environment-specific database using parameter first
CREATE OR ALTER DATABASE QUICKSTART_{{environment}};


-- To monitor data pipeline's completion
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;

USE DATABASE QUICKSTART_{{environment}};
-- Now that we're using the correct database, create the schemas
CREATE OR ALTER SCHEMA IDENTIFIER($DATABASE_NAME).bronze;
CREATE OR ALTER SCHEMA IDENTIFIER($DATABASE_NAME).silver; 
CREATE OR ALTER SCHEMA IDENTIFIER($DATABASE_NAME).gold;

-- Explicitly set context before creating schema objects



-- Schema level objects
CREATE OR REPLACE FILE FORMAT bronze.json_format TYPE = 'json';
CREATE OR ALTER STAGE bronze.raw;

-- Copy file from GitHub to internal stage with full context
COPY FILES INTO @bronze.raw FROM @quickstart_common.public.quickstart_repo/branches/main/data/airport_list.json;

LIST @bronze.raw;