CREATE OR REPLACE PROCEDURE CDP_DB.CDP_SCHEMA.GET_YAML("STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
        yaml_file string;
        m_sql string;
    BEGIN
        yaml_file := (SELECT listagg (DISTINCT METADATA$FILENAME,'','') FROM @cdp_stage/);
        return yaml_file;
    END';

CREATE OR REPLACE PROCEDURE CDP_DB.CDP_SCHEMA.CAPTURE_CUSTOMER_PREFERENCES("USER_QUERY" VARCHAR(16777216), "GENERATED_SQL" VARCHAR(16777216), "USER_ACTION" VARCHAR(16777216), "CHART_PREFERENCE" VARCHAR(16777216), "CHART_TYPE" VARCHAR(16777216) DEFAULT null, "X_AXIS" VARCHAR(16777216) DEFAULT null, "Y_AXIS" VARCHAR(16777216) DEFAULT null, "LEGEND_KEY" VARCHAR(16777216) DEFAULT null, "CAMPAIGN_NAME" VARCHAR(16777216) DEFAULT null, "CAMPAIGN_CONSUMER" VARCHAR(16777216) DEFAULT null, "CAMPAIGN_FREQUENCY" VARCHAR(16777216) DEFAULT null)
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
INSERT_QUERY STRING;

BEGIN

INSERT INTO saved_queries VALUES (save_queries.nextval,:user_query,:generated_sql,:user_action,:chart_preference,:chart_type,
                                 :x_axis,:y_axis,:legend_key,:campaign_name,:campaign_consumer,:campaign_frequency   
                                 );

RETURN True;

END';