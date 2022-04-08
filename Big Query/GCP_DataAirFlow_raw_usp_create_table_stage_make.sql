CREATE OR REPLACE PROCEDURE `dataairflow-poc.raw.usp_create_table_stage_make`()
BEGIN
CREATE or REPLACE TABLE `dataairflow-poc.stage.make`
(
	MakeID INT64 NOT NULL,
	MakeName STRING,
	MakeCountry STRING
) ;
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message, @@error.statement_text;
END;