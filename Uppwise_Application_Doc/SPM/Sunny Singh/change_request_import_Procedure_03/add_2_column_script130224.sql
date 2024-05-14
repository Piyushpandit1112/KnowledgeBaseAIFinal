


--add the 2 column in temp_ImportCostPhasing
 
ALTER TABLE temp_ImportCostPhasing
ADD DG_CDR nvarchar(100) DEFAULT NULL

 

ALTER TABLE temp_ImportCostPhasing
ADD DSC_CDR nvarchar(1000) DEFAULT NULL
 

