-- Functions for Tier0 Oracle schema

-- Function to check for zero state
CREATE OR REPLACE FUNCTION checkForZeroState (value IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
    IF value = 0 THEN
        RETURN 0;
    ELSE
        RETURN NULL;
    END IF;
END checkForZeroState;
/

-- Function to check for zero/one state
CREATE OR REPLACE FUNCTION checkForZeroOneState (value IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
    IF value = 0 THEN
        RETURN 0;
    ELSIF value = 1 THEN
        RETURN 1;
    ELSE
        RETURN NULL;
    END IF;
END checkForZeroOneState;
/

-- Function to check for zero/one/two state
CREATE OR REPLACE FUNCTION checkForZeroOneTwoState (value IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
    IF value = 0 THEN
        RETURN 0;
    ELSIF value = 1 THEN
        RETURN 1;
    ELSIF value = 2 THEN
        RETURN 2;
    ELSE
        RETURN NULL;
    END IF;
END checkForZeroOneTwoState;
/ 