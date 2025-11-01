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

-- CREATE OR REPLACE SECRET git_secret
--   TYPE = password
--   USERNAME = '<github username>'
--   PASSWORD = '<personal access token>';

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/asingh1989') -- INSERT YOUR GITHUB USERNAME HERE
  --  ALLOWED_AUTHENTICATION_SECRETS = (git_secret)
  ENABLED = TRUE;

-- Git repository object is similar to external stag
CREATE OR REPLACE GIT REPOSITORY quickstart_common.public.quickstart_repo
  API_INTEGRATION = git_api_integration
  --GIT_CREDENTIALS = git_secret
  ORIGIN = 'https://github.com/asingh1989/sfguide-getting-started-with-snowflake-devops'; -- INSERT URL OF FORKED REPO HERE




-- Create environment-specific database using parameter first=
CREATE OR ALTER DATABASE QUICKSTART_{{environment}};


-- To monitor data pipeline's completion
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;

USE DATABASE QUICKSTART_{{environment}};
-- Now that we're using the correct database, create the schemas
CREATE OR ALTER SCHEMA bronze;
CREATE OR ALTER SCHEMA silver; 
CREATE OR ALTER SCHEMA gold;

-- Explicitly set context before creating schema objects



-- Schema level objects
CREATE OR REPLACE FILE FORMAT bronze.json_format TYPE = 'json';
CREATE OR ALTER STAGE bronze.raw;

-- Copy file from GitHub to internal stage with full context
COPY FILES INTO @bronze.raw FROM @quickstart_common.public.quickstart_repo/branches/main/data/airport_list.json;

create or ALTER TABLE QUICKSTART_{{environment}}.SILVER.AIRPORTS (
	CITY_NAME VARCHAR(500),
	IATA_CODE VARCHAR(500)
);

LIST @bronze.raw;