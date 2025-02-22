/*-----------------------------------------------------------------------------
  Hands-On Lab: Intro to Data Engineering with Notebooks
  Script:       bootstrap.sql
  Author:       Jeremiah Hansen (Improved Version)
  Last Updated: 2024-06-11 (Update credentials & details as needed)
-----------------------------------------------------------------------------*/

/*
   IMPORTANT: Replace the placeholder values for GitHub credentials and URLs
   with your actual GitHub username, personal access token, and repository details.
*/

-- ----------------------------------------------------------------------------
-- Set Variables for Dynamic Use
-- ----------------------------------------------------------------------------
SET MY_USER = CURRENT_USER();
SET GITHUB_SECRET_USERNAME = 'your_github_username';      -- update with your GitHub username
SET GITHUB_SECRET_PASSWORD = 'your_personal_access_token';  -- update with your personal access token
SET GITHUB_URL_PREFIX = 'https://github.com/your_github_username';  -- update accordingly
SET GITHUB_REPO_ORIGIN = 'https://github.com/your_github_username/sfguide-data-engineering-with-notebooks.git';  -- update accordingly

-- ----------------------------------------------------------------------------
-- ACCOUNT LEVEL OBJECTS (Run as ACCOUNTADMIN)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Create a demo role and assign it to SYSADMIN and the current user
CREATE OR REPLACE ROLE DEMO_ROLE;
GRANT ROLE DEMO_ROLE TO ROLE SYSADMIN;
GRANT ROLE DEMO_ROLE TO USER IDENTIFIER($MY_USER);

-- Grant necessary account-level privileges to DEMO_ROLE
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DEMO_ROLE;

-- Create a dedicated database and assign ownership to DEMO_ROLE
CREATE OR REPLACE DATABASE DEMO_DB;
GRANT OWNERSHIP ON DATABASE DEMO_DB TO ROLE DEMO_ROLE;

-- Create a warehouse with autosuspend/resume settings and assign ownership
CREATE OR REPLACE WAREHOUSE DEMO_WH 
    WAREHOUSE_SIZE = XSMALL 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE DEMO_WH TO ROLE DEMO_ROLE;

-- ----------------------------------------------------------------------------
-- DATABASE LEVEL OBJECTS (Run as DEMO_ROLE)
-- ----------------------------------------------------------------------------
USE ROLE DEMO_ROLE;
USE WAREHOUSE DEMO_WH;
USE DATABASE DEMO_DB;

-- Create required schemas for integrations and environments
CREATE OR REPLACE SCHEMA INTEGRATIONS;
CREATE OR REPLACE SCHEMA DEV_SCHEMA;
CREATE OR REPLACE SCHEMA PROD_SCHEMA;

-- ----------------------------------------------------------------------------
-- EXTERNAL INTEGRATION & GIT CONFIGURATION (Within INTEGRATIONS Schema)
-- ----------------------------------------------------------------------------
USE SCHEMA INTEGRATIONS;

-- Create an external stage for the raw data from Frostbyte (S3)
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/';

-- Create a secret for GitHub credentials at the schema level
CREATE OR REPLACE SECRET DEMO_GITHUB_SECRET
  TYPE = password
  USERNAME = $GITHUB_SECRET_USERNAME
  PASSWORD = $GITHUB_SECRET_PASSWORD;

-- Create an API integration for GitHub. This integration depends on the secret above.
CREATE OR REPLACE API INTEGRATION DEMO_GITHUB_API_INTEGRATION
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ($GITHUB_URL_PREFIX)
  ALLOWED_AUTHENTICATION_SECRETS = (DEMO_GITHUB_SECRET)
  ENABLED = TRUE;

-- Configure the Git repository integration to connect your Snowflake instance with your GitHub repository.
CREATE OR REPLACE GIT REPOSITORY DEMO_GIT_REPO
  API_INTEGRATION = DEMO_GITHUB_API_INTEGRATION
  GIT_CREDENTIALS = DEMO_GITHUB_SECRET
  ORIGIN = $GITHUB_REPO_ORIGIN;

-- ----------------------------------------------------------------------------
-- ADDITIONAL OBJECTS (Example: Create Event Table)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
-- Add further DDL commands as needed (for example, creating an event table).
-- CREATE OR REPLACE TABLE EVENT_TABLE (...);

-- End of bootstrap script
