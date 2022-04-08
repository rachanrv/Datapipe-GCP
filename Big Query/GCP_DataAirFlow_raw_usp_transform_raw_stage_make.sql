CREATE OR REPLACE PROCEDURE `dataairflow-poc.raw.usp_transform_raw_stage_make`()
BEGIN
INSERT `dataairflow-poc.stage.make`(MakeID,MakeName,MakeCountry	)
select 
cast(MakeID as int64) as MakeID ,
MakeName,
MakeCountry	 
from 
`dataairflow-poc.raw.make`
where MakeID <> 'MakeID' ;

-- truncate source table
truncate table `dataairflow-poc.raw.make`;
END;