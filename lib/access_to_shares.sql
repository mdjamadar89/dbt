-- Use these videos as instructions for the setup:
-- https://www.loom.com/share/aa8895121013431c8b15088be687fc99
-- https://www.loom.com/share/fdccef7020e7495fbeda4d8d75cb63f5
-- Login to the reader account and run this (after updating the script for the customer name)

USE ROLE accountadmin;

CREATE OR REPLACE WAREHOUSE CUSTOMER_NAME_WH
    WAREHOUSE_SIZE = XSMALL
    WAREHOUSE_TYPE = standard
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    AUTO_SUSPEND = 600
    AUTO_RESUME = true
    INITIALLY_SUSPENDED = true
;
GRANT USAGE ON WAREHOUSE CUSTOMER_NAME_WH TO ROLE SYSADMIN;

CREATE USER IF NOT EXISTS CUSTOMER_NAME_USER 
    default_role = sysadmin
    default_warehouse = CUSTOMER_NAME_WH
    MUST_CHANGE_PASSWORD = TRUE
    password = '' -- put a strong password here and pass it on to the customer
;
GRANT ROLE sysadmin TO USER CUSTOMER_NAME_USER;

CREATE OR REPLACE DATABASE verb_reporting from share lpa59350.CUSTOMER_SHARE_NAME;

GRANT IMPORTED PRIVILEGES ON DATABASE verb_reporting TO ROLE sysadmin;
