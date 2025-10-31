USE ROLE ACCOUNTADMIN;

CREATE OR ALTER WAREHOUSE QUICKSTART_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 300 
  AUTO_RESUME= TRUE;

-- Use the warehouse immediately after creating it
USE WAREHOUSE QUICKSTART_WH;

-- Separate database for git repository
CREATE OR ALTER DATABASE QUICKSTART_COMMON;

use DATABASE QUICKSTART_COMMON;
-- API integration is needed for GitHub integration
-- API integration is needed for GitHub integratio
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/asingh1989') -- INSERT YOUR GITHUB USERNAME HERE
  ENABLED = TRUE;

-- Git repository object is similar to external stag
CREATE OR REPLACE GIT REPOSITORY quickstart_common.public.quickstart_repo
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/asingh1989/sfguide-getting-started-with-snowflake-devops'; -- INSERT URL OF FORKED REPO HERE



