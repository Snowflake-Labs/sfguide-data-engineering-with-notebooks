/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       setup.sql
Author:       Jeremiah Hansen
Last Updated: 2/2/2026
-----------------------------------------------------------------------------*/


-- ----------------------------------------------------------------------------
-- Create the account level objects (ACCOUNTADMIN part)
-- ----------------------------------------------------------------------------
SET MY_USER = CURRENT_USER();
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

-- This is a schema level object
CREATE OR REPLACE NETWORK RULE PYPI_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('pypi.org', 'pypi.python.org', 'pythonhosted.org', 'files.pythonhosted.org');

-- This is an account level object
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_ACCESS_INTEGRATION
ALLOWED_NETWORK_RULES = (PYPI_NETWORK_RULE)
ENABLED = true;


-- ----------------------------------------------------------------------------
-- Create the event table
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS;
GRANT SELECT ON EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS TO ROLE DEMO_ROLE;
GRANT INSERT ON EVENT TABLE DEMO_DB.INTEGRATIONS.DEMO_EVENTS TO ROLE DEMO_ROLE;

ALTER ACCOUNT SET EVENT_TABLE = DEMO_DB.INTEGRATIONS.DEMO_EVENTS;
ALTER DATABASE DEMO_DB SET LOG_LEVEL = INFO;
