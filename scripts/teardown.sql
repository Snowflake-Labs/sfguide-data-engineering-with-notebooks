/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       teardown.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP API INTEGRATION DEMO_GITHUB_API_INTEGRATION;
DROP DATABASE DEMO_DB;
DROP WAREHOUSE DEMO_WH;
DROP ROLE DEMO_ROLE;

-- Drop the weather share
DROP DATABASE FROSTBYTE_WEATHERSOURCE;

-- Remove the "dev" branch in your repo
