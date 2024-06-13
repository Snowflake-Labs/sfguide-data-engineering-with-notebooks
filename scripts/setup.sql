/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       bootstrap.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/

SET MY_USER = CURRENT_USER();
SET GITHUB_SECRET_USERNAME = 'username';
SET GITHUB_SECRET_PASSWORD = 'personal access token';
SET GITHUB_URL_PREFIX = 'https://github.com/username';
SET GITHUB_REPO_ORIGIN = 'https://github.com/username/sfguide-data-engineering-with-notebooks.git';


-- ----------------------------------------------------------------------------
-- Create the account level objects (ACCOUNTADMIN part)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
CREATE OR REPLACE ROLE DEMO_ROLE;
GRANT ROLE DEMO_ROLE TO ROLE SYSADMIN;
GRANT ROLE DEMO_ROLE TO USER IDENTIFIER($MY_USER);

GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DEMO_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DEMO_ROLE;

-- Databases
CREATE OR REPLACE DATABASE DEMO_DB;
GRANT OWNERSHIP ON DATABASE DEMO_DB TO ROLE DEMO_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE DEMO_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE DEMO_WH TO ROLE DEMO_ROLE;


-- ----------------------------------------------------------------------------
-- Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE DEMO_ROLE;
USE WAREHOUSE DEMO_WH;
USE DATABASE DEMO_DB;

-- Schemas
CREATE OR REPLACE SCHEMA INTEGRATIONS;
CREATE OR REPLACE SCHEMA DEV_SCHEMA;
CREATE OR REPLACE SCHEMA PROD_SCHEMA;

USE SCHEMA INTEGRATIONS;

-- External Frostbyte objects
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;

-- Secrets (schema level)
CREATE OR REPLACE SECRET DEMO_GITHUB_SECRET
  TYPE = password
  USERNAME = $GITHUB_SECRET_USERNAME
  PASSWORD = $GITHUB_SECRET_PASSWORD;

-- API Integration (account level)
-- This depends on the schema level secret!
CREATE OR REPLACE API INTEGRATION DEMO_GITHUB_API_INTEGRATION
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ($GITHUB_URL_PREFIX)
  ALLOWED_AUTHENTICATION_SECRETS = (DEMO_GITHUB_SECRET)
  ENABLED = TRUE;

-- Create the "dev" branch in your repo

-- Git Repository
CREATE OR REPLACE GIT REPOSITORY DEMO_GIT_REPO
  API_INTEGRATION = DEMO_GITHUB_API_INTEGRATION
  GIT_CREDENTIALS = DEMO_GITHUB_SECRET
  ORIGIN = $GITHUB_REPO_ORIGIN;


-- ----------------------------------------------------------------------------
-- Create the event table
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS;
GRANT SELECT ON EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS TO ROLE DEMO_ROLE;
GRANT INSERT ON EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS TO ROLE DEMO_ROLE;

ALTER ACCOUNT SET EVENT_TABLE = DEMO_DB.INTEGRATIONS.DEMO_EVENTS;
ALTER DATABASE DEMO_DB SET LOG_LEVEL = INFO;
