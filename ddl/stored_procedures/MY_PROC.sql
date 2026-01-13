-- This will update the procedure to the latest definition on every run.
CREATE OR REPLACE PROCEDURE SILVER.MY_PROC_123456(PARAM_1 VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    RETURN 'Hello, ' || PARAM_1;
END;
$$;