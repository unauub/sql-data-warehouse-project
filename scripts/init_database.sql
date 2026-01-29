/*
------------------------------------------------------------
Create Database Schemas
------------------------------------------------------------

Purpose:
This script creates the core schemas used in the data warehouse
following a Medallion Architecture approach:

- bronze
- silver
- gold

Note:
The database itself was created manually using pgAdmin.
This script assumes the database already exists.
------------------------------------------------------------
*/

-- Create schemas for the data warehouse layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
