CREATE OR REPLACE PROCEDURE `dataairflow-poc.stage.usp_upd_ins_stag_base_make`()
BEGIN
    -- update existing records
    MERGE `dataairflow-poc.base.make`   t1
    USING `dataairflow-poc.stage.make` t2
    ON t1.Makeid       = t2.Makeid
    WHEN MATCHED THEN
        UPDATE set t1.MakeName= t2.MakeName, 
        t1.MakeCountry  = t2.MakeCountry,
        t1.UpdatedDate =CURRENT_DATE();
    
    INSERT `dataairflow-poc.base.make`
	(MakeID,MakeName,MakeCountry,CreatedDate)
    SELECT t1.*,CURRENT_DATE()
    FROM `dataairflow-poc.stage.make` as t1 
    LEFT OUTER JOIN `dataairflow-poc.base.make`  as t2
    ON  t1.Makeid       = t2.Makeid 
    AND t1.MakeName     = t2.MakeName 
    AND t1.MakeCountry  = t2.MakeCountry 
    WHERE t2.Makeid IS NULL AND t2.MakeName IS NULL 
	AND t2.MakeCountry IS NULL;


EXCEPTION WHEN ERROR THEN
  SELECT @@error.message, @@error.statement_text;
END;