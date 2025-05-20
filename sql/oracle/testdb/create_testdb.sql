-- Test Database Schema
-- This file creates the test database tables for Oracle

-- Create test_tablea
CREATE TABLE test_tablea (
    column1 NUMBER(11),
    column2 NUMBER(11),
    column3 VARCHAR2(255)
)
/

-- Create test_tableb
CREATE TABLE test_tableb (
    column1 VARCHAR2(255),
    column2 NUMBER(11),
    column3 VARCHAR2(255)
)
/

-- Create test_tablec
CREATE TABLE test_tablec (
    column1 VARCHAR2(255),
    column2 VARCHAR2(255),
    column3 VARCHAR2(255)
)
/

-- Create test_bigcol
CREATE TABLE test_bigcol (
    column1 NUMBER(35)
)
/

-- Add comments to tables
COMMENT ON TABLE test_tablea IS 'Test table A with integer and varchar columns'
/

COMMENT ON TABLE test_tableb IS 'Test table B with varchar and integer columns'
/

COMMENT ON TABLE test_tablec IS 'Test table C with varchar columns'
/

COMMENT ON TABLE test_bigcol IS 'Test table with large decimal column'
/
