-- Test Database Schema
-- This file creates the test database tables for MariaDB

-- Drop existing tables if they exist
DROP TABLE IF EXISTS test_tablea;
DROP TABLE IF EXISTS test_tableb;
DROP TABLE IF EXISTS test_tablec;
DROP TABLE IF EXISTS test_bigcol;

-- Create test_tablea
CREATE TABLE test_tablea (
    column1 INT,
    column2 INT,
    column3 VARCHAR(255)
) ENGINE=InnoDB COMMENT='Test table A with integer and varchar columns';

-- Create test_tableb
CREATE TABLE test_tableb (
    column1 VARCHAR(255),
    column2 INT,
    column3 VARCHAR(255)
) ENGINE=InnoDB COMMENT='Test table B with varchar and integer columns';

-- Create test_tablec
CREATE TABLE test_tablec (
    column1 VARCHAR(255),
    column2 VARCHAR(255),
    column3 VARCHAR(255)
) ENGINE=InnoDB COMMENT='Test table C with varchar columns';

-- Create test_bigcol
CREATE TABLE test_bigcol (
    column1 DECIMAL(35,0)
) ENGINE=InnoDB COMMENT='Test table with large decimal column';

-- Add column comments using ALTER TABLE
ALTER TABLE test_tablea 
    MODIFY column1 INT COMMENT 'First integer column',
    MODIFY column2 INT COMMENT 'Second integer column',
    MODIFY column3 VARCHAR(255) COMMENT 'Varchar column';

ALTER TABLE test_tableb 
    MODIFY column1 VARCHAR(255) COMMENT 'First varchar column',
    MODIFY column2 INT COMMENT 'Integer column',
    MODIFY column3 VARCHAR(255) COMMENT 'Second varchar column';

ALTER TABLE test_tablec 
    MODIFY column1 VARCHAR(255) COMMENT 'First varchar column',
    MODIFY column2 VARCHAR(255) COMMENT 'Second varchar column',
    MODIFY column3 VARCHAR(255) COMMENT 'Third varchar column';

ALTER TABLE test_bigcol 
    MODIFY column1 DECIMAL(35,0) COMMENT 'Large decimal column';
