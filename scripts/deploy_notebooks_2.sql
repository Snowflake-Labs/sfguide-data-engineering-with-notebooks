--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       deploy_notebooks.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/

-- See https://docs.snowflake.com/en/LIMITEDACCESS/execute-immediate-from-template

-- Create the Notebooks
--USE SCHEMA {{env}}_SCHEMA;

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"DEMO_DB"."{{env}}_SCHEMA"."{{env}}my_file"')
    FROM '@"DEMO_DB"."INTEGRATIONS"."DEMO_GIT_REPO"/branches/"{{branch}}"/notebooks/my_file/'
    QUERY_WAREHOUSE = 'DEMO_WH'
    MAIN_FILE = 'my_file.ipynb';

ALTER NOTEBOOK "DEMO_DB"."{{env}}_SCHEMA"."{{env}}my_file" ADD LIVE VERSION FROM LAST;
