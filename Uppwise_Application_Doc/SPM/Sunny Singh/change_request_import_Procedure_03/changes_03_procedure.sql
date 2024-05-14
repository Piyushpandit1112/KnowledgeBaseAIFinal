USE [SPMSella]
GO
/****** Object:  StoredProcedure [dbo].[ImportCostPhasing]    Script Date: 12-02-2024 18:18:38 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[ImportCostPhasing]
AS
  BEGIN

      declare  @maxid integer = 0
	  declare  @error integer = 0
      -- create user start here
        CREATE TABLE #Log
        (
		   ID_ int  identity(1,1) Primary key,
           Date_        nvarchar(20),
           Hour_  nvarchar(20),
           Description_  NVARCHAR(400),
           Values_   NVARCHAR(400),
		   Comment_ NVARCHAR(100)
        )

   insert into #Log(Date_,Hour_,Description_,Values_) values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Start import','3 – Consuntivi Zucchetti per Progetto')

   select @maxid = MAX(ID) from temp_ImportCostPhasing


-----insert codes start --------
		Create Table #InsertCodes
		(
		 idL0  BIGINT Identity(1,1),
		   CLASSIFICAZIONE_COGE          NVARCHAR(100),
           PROGETTO NVARCHAR(100),
		   GENNAIO numeric(18,9),
		   FEBBRAIO numeric(18,9),
		   MARZO numeric(18,9),
		   APRILE numeric(18,9),
		   MAGGIO numeric(18,9),
		   GIUGNO numeric(18,9),
		   LUGLIO numeric(18,9),
		   AGOSTO numeric(18,9),
		   SETTEMBRE numeric(18,9),
		   OTTOBRE numeric(18,9),
		   NOVEMBRE numeric(18,9),
		   DICEMBRE numeric(18,9),
		   ANNO Integer,
		  FORNITORE_CODICE nvarchar(100),
		  FORNITORE_DESCRIZIONE Nvarchar(1000),
		  DG_CDR nvarchar(100),
		  DSC_CDR nvarchar(1000)

		)
		Update temp_ImportCostPhasing SET FORNITORE_CODICE=null where FORNITORE_CODICE=''
       Update temp_ImportCostPhasing SET FORNITORE_DESCRIZIONE=null where FORNITORE_DESCRIZIONE=''

	    insert into #InsertCodes(CLASSIFICAZIONE_COGE,PROGETTO,GENNAIO,FEBBRAIO,MARZO,APRILE,MAGGIO,
		GIUGNO,LUGLIO,AGOSTO,SETTEMBRE,OTTOBRE,NOVEMBRE,DICEMBRE,ANNO,FORNITORE_CODICE,FORNITORE_DESCRIZIONE,DG_CDR,DSC_CDR 
		) 
         select CLASSIFICAZIONE_COGE,PROGETTO,GENNAIO,FEBBRAIO,MARZO,APRILE,MAGGIO,
		       GIUGNO,LUGLIO,AGOSTO,SETTEMBRE,OTTOBRE,NOVEMBRE,DICEMBRE,ANNO,FORNITORE_CODICE,FORNITORE_DESCRIZIONE,DG_CDR,DSC_CDR 
		       from temp_ImportCostPhasing x 
        where x.ID = @maxid 
		--AND X.PROGETTO='0022806' 
		--and X.CLASSIFICAZIONE_COGE='50I3SW5OT0110'

		Update #InsertCodes SET FORNITORE_CODICE=null where FORNITORE_CODICE=''
 
	  DECLARE @Inserti INTEGER=1
      DECLARE @add INTEGER = 0
	  Declare @insertedValue nvarchar(400) = ''

	  declare @CLASSIFICAZIONE_COGE nvarchar(100) = ''
	  declare @PROGETTO nvarchar(100) = ''

	  declare @GENNAIO numeric(18,9) = null
	  declare @FEBBRAIO numeric(18,9) = null
	  declare @MARZO numeric(18,9) = null
	  declare @APRILE numeric(18,9) = null
	  declare @MAGGIO numeric(18,9) = null
	  declare @GIUGNO numeric(18,9) = null
	  declare @LUGLIO numeric(18,9) = null
	  declare @AGOSTO numeric(18,9) = null
	  declare @SETTEMBRE numeric(18,9) = null
	  declare @OTTOBRE numeric(18,9) = null
      declare @NOVEMBRE numeric(18,9) = null
	  declare @DICEMBRE numeric(18,9) = null

	  declare @FORNITORE_CODICE nvarchar(100) = ''
	  declare @FORNITORE_DESCRIZIONE nvarchar(100) = ''

	  declare @ANNO integer = null
	  declare @c_prog bigint = null
	  declare @Ass_id bigint = null

	  declare @Org_GENNAIO numeric(18,9) = null
	  declare @Org_FEBBRAIO numeric(18,9) = null
	  declare @Org_MARZO numeric(18,9) = null
	  declare @Org_APRILE numeric(18,9) = null
	  declare @Org_MAGGIO numeric(18,9) = null
	  declare @Org_GIUGNO numeric(18,9) = null
	  declare @Org_LUGLIO numeric(18,9) = null
	  declare @Org_AGOSTO numeric(18,9) = null
	  declare @Org_SETTEMBRE numeric(18,9) = null
	  declare @Org_OTTOBRE numeric(18,9) = null
      declare @Org_NOVEMBRE numeric(18,9) = null
	  declare @Org_DICEMBRE numeric(18,9) = null
	  
	  declare @AssingmentDate date = null
	  declare @updated integer = 0

	  declare @capexopex integer = 0
	  declare @elementtype nvarchar(10) = ''
	  declare @taskstartdate datetime = null
	  declare @taskfinishdate datetime = null
	  declare @currency nvarchar(10) = null
	  declare @printYear nvarchar(10) = ''
	  DECLARE @ElementstartDate DATETIME
	  DECLARE @ElementFinishDate DATETIME
	  Declare @MaxActualPhaingstartDate DATETIME
	  DECLARE @MaxActualPhaingstartDateINC DATETIME
      DECLARE @MAXActualPhasingFinishDate DATETIME
	  DECLARE @MAXActualPhasingFinishDateInc DATETIME
	  DECLARE @CheckPhasingStartDate DATETIME
	  DECLARE @CheckPhasingEndDate DATETIME
	  declare @phasingStartDate datetime = null
	  declare @phasingFinishDate datetime = null

	  declare @sum decimal(18,2) = 0
	  DECLARE @Sumforecast AS decimal(18,2)=0

	  declare @DG_CDR as nvarchar(100)
	  declare @DSC_CDR as nvarchar(1000)

      SELECT @add = Count(*) FROM  #InsertCodes

        WHILE @Inserti <= @add
         BEGIN
	  BEGIN TRY  

	  set @CLASSIFICAZIONE_COGE  = ''
	  set @PROGETTO  = ''

	  set @GENNAIO  = null
	  set @FEBBRAIO  = null
	  set @MARZO  = null
	  set @APRILE  = null
	  set @MAGGIO  = null
	  set @GIUGNO  = null
	  set @LUGLIO  = null
	  set @AGOSTO  = null
	  set @SETTEMBRE  = null
	  set @OTTOBRE  = null
      set @NOVEMBRE  = null
	  set @DICEMBRE  = null


	  set @Org_GENNAIO  = null
	  set @Org_FEBBRAIO  = null
	  set @Org_MARZO  = null
	  set @Org_APRILE  = null
	  set @Org_MAGGIO  = null
	  set @Org_GIUGNO  = null
	  set @Org_LUGLIO  = null
	  set @Org_AGOSTO  = null
	  set @Org_SETTEMBRE  = null
	  set @Org_OTTOBRE  = null
      set @Org_NOVEMBRE  = null
	  set @Org_DICEMBRE  = null


	  set @ANNO  = null
	  set @c_prog = null
	  set @Ass_id = null
	  set @updated = 0
	  set @sum = 0
	  set @AssingmentDate = null

	  set @FORNITORE_CODICE = ''
	  set @FORNITORE_DESCRIZIONE = ''
	  set @DG_CDR = ''
	  set @DSC_CDR = ''
	
	   SELECT @PROGETTO = PROGETTO, @CLASSIFICAZIONE_COGE = CLASSIFICAZIONE_COGE,
	   @GENNAIO = GENNAIO,@FEBBRAIO = FEBBRAIO,@MARZO = MARZO,@APRILE = APRILE,@MAGGIO = MAGGIO,
	   @GIUGNO = GIUGNO, @LUGLIO = LUGLIO,@AGOSTO = AGOSTO,@SETTEMBRE = SETTEMBRE,@OTTOBRE = OTTOBRE,
	   @NOVEMBRE = NOVEMBRE,@DICEMBRE = DICEMBRE , @ANNO = ANNO,@FORNITORE_CODICE =FORNITORE_CODICE,@FORNITORE_DESCRIZIONE = FORNITORE_DESCRIZIONE,@DG_CDR = DG_CDR,
	    @DSC_CDR = DSC_CDR
	   FROM #InsertCodes WHERE idL0= @Inserti

	 set  @printYear =  cast(@ANNO as nvarchar(10))

	  select @c_prog = C_PROG from  BPM_OBJECTS where OBJ_CODE	= @PROGETTO and F_ID = 10

	  if @c_prog > 0
	  begin
    
		if (select count(C_CLASSE) from CLASSE_DI_COSTO_T043 where C_CLASSE = @CLASSIFICAZIONE_COGE) > 0
		begin
		IF @FORNITORE_CODICE is not null 
		BEGIN
			IF (select count(C_CLI) from CLI_T023 where C_CLI = @FORNITORE_CODICE)  = 0
			begin
				insert into CLI_T023(C_CLI,S_CLI,F_TIPO,C_AZD)
				  values(@FORNITORE_CODICE,@FORNITORE_DESCRIZIONE,'F',2)
			END
		END

		if @FORNITORE_CODICE is null OR @FORNITORE_CODICE='' 
		BEGIN
		 SELECT @Ass_id = ASS_ID from Cost_Rev_Asso_Task where PROJ_ID  = @c_prog and ELEMENT_ID  = @CLASSIFICAZIONE_COGE and field1 is null
		END
		ELSE
		BEGIN
		 SELECT @Ass_id = ASS_ID from Cost_Rev_Asso_Task where PROJ_ID  = @c_prog and ELEMENT_ID  = @CLASSIFICAZIONE_COGE and field1 = @FORNITORE_CODICE
		END
		

		 if @Ass_id > 0 
		 begin
		   --Update here assignment----
		  set @taskstartdate = null
		  set @taskfinishdate = null

		    select @taskstartdate = CAST(TASK_START_DATE AS date),@taskfinishdate = CAST(TASK_FINISH_DATE AS date) from MSP_TASKS where PROJ_ID = @c_prog AND TASK_UID=1
		
			SET @ElementstartDate= null 
			SET @ElementFinishDate=null 
			SET @CheckPhasingStartDate =null
	        SET @CheckPhasingEndDate =null 
			Select @ElementstartDate=CAST(STARTDATE AS date), @ElementFinishDate= CAST(FINISHDATE AS date) from Cost_Rev_Asso_Task  where ASS_ID =@Ass_id AND PROJ_ID=@c_prog

			SELECT @CheckPhasingStartDate=CAST(MIN(STARTDATE) AS date),@CheckPhasingEndDate=CAST(MAX(FINISHDATE) AS date)  from Tab_Cost_Actual_Phasing  where ASS_ID =@Ass_id

			IF @ElementFinishDate < CAST(@taskfinishdate AS date) OR @ElementstartDate> CAST(@taskstartdate AS date) OR @CheckPhasingStartDate<>dbo.first_day(@ElementstartDate) OR @CheckPhasingEndDate<>dbo.last_day(@ElementFinishDate)
			BEGIN
			DECLARE @ElementFinishDatetostartDate AS DATETIME
			SET @ElementFinishDatetostartDate=null 
			SET @MAXActualPhasingFinishDateInc=null 
			SELECT @ElementFinishDatetostartDate= DATEADD(month, 1, @ElementFinishDate)
			   exec PM_AutoCostsPhasing 2,@Ass_id,'TAB_Cost_WORK_PHASING',0,@ElementFinishDatetostartDate,@taskfinishdate 
			   SET @MaxActualPhaingstartDate=null 
			   SET @MAXActualPhasingFinishDate=null
			   Select @MAXActualPhasingFinishDate= MAX(FINISHDATE), @MaxActualPhaingstartDate=MIN(STARTDATE)  from Tab_Cost_Actual_Phasing where ASS_ID =@Ass_id
			    Select @MAXActualPhasingFinishDateInc= DATEADD(month, 1, MAX(FINISHDATE)), @MaxActualPhaingstartDateINC= DATEADD(month, -1, MIN(STARTDATE))  from Tab_Cost_Actual_Phasing where ASS_ID =@Ass_id
				
			   if @MAXActualPhasingFinishDate < @taskfinishdate
			   BEGIN
			      exec PM_AutoCostsPhasing 2,@Ass_id,'Tab_Cost_Actual_Phasing',0,@MAXActualPhasingFinishDateInc,@taskfinishdate
			   END
			   IF @MaxActualPhaingstartDate > @taskstartdate
			   BEGIN
				   exec PM_AutoCostsPhasing 2,@Ass_id,'Tab_Cost_Actual_Phasing',0,@taskstartdate,@MaxActualPhaingstartDateINC
				   exec PM_AutoCostsPhasing 2,@Ass_id,'TAB_Cost_WORK_PHASING',0,@taskstartdate,@MaxActualPhaingstartDateINC
			   END
			   INSERT INTO RestoreUpdateASSignmentCode(ASS_ID,TASK_finishDate)
			   SELECT @Ass_id,@taskfinishdate
			END

			-- END BY Pradeep Kumar

		 select @Org_GENNAIO =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 1
		 
		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 1, 31)

		 if (ROUND(ISNULL(@Org_GENNAIO,0),2) <> ROUND(ISNULL(@GENNAIO,0),2))
		 begin 
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		 if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 1) > 0 )
		 begin
		  update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @GENNAIO, ELEMENT_VALUE_VL = @GENNAIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 1
		 end
		 ELSE
			 BEGIN
				SELECT @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
				INSERT INTO Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				values(@Ass_id, dbo.first_day(@AssingmentDate) , dbo.last_day(@phasingFinishDate),@GENNAIO,@GENNAIO)
				IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 1)
				BEGIN
					insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
				END
			 
		    END
		 set @updated = 1
		 END
		 ELSE
			 BEGIN
			  INSERT INTO #Log(Date_,Hour_,Description_,Values_,Comment_)
			  VALUES(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - GENNAIO out of project duration')  
			 END
		 end
	
			
         select @AssingmentDate = DATEFROMPARTS (@ANNO, 2, 28)
	     select @Org_FEBBRAIO   =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 2
		 if (ROUND(ISNULL(@Org_FEBBRAIO,0),2) <> ROUND(ISNULL(@FEBBRAIO,0),2)) 
		 begin

			 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
			 begin
			  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 2) > 0 )
				 BEGIN
					update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @FEBBRAIO, ELEMENT_VALUE_VL = @FEBBRAIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 2
				 END
		
			 else
			 BEGIN
				  select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
				  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@FEBBRAIO,@FEBBRAIO)
				  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 2)
				  BEGIN
					  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
				  END
			 END

			 set @updated = 1
			 end
			 ELSE
				 BEGIN
				  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
				  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - FEBBRAIO out of project duration')  
				 END
		 end

     
		 SELECT @AssingmentDate = DATEFROMPARTS (@ANNO, 3, 31)
		 SELECT @Org_MARZO =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 3
		 if (ROUND(ISNULL(@Org_MARZO,0),2) <> ROUND(ISNULL(@MARZO,0),2))
		 begin
			 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)

			 begin
					 if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 3) > 0 )
					 begin
						 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @MARZO, ELEMENT_VALUE_VL = @MARZO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 3
	  				 end

				 ELSE
				 BEGIN
					  select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
					  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@MARZO,@MARZO)

					  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 3)
					  BEGIN
					  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
					  END
		 
				 end
				 SET @updated = 1
			 end
			 else
				 BEGIN
				  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
				  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - MARZO out of project duration')  
				 END
		 end

		 SELECT @AssingmentDate = DATEFROMPARTS (@ANNO, 4, 30)
		 select @Org_APRILE =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 4
		 if (ROUND(ISNULL(@Org_APRILE,0),2) <> ROUND(ISNULL(@APRILE,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		  IF ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 4) > 0 )
		 BEGIN
			 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @APRILE, ELEMENT_VALUE_VL = @APRILE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 4
		 END
         ELSE
		 BEGIN
			  SELECT @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
			  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
			  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@APRILE,@APRILE)
				if NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 4)
				BEGIN
					insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
				END
		 END
		 set @updated = 1
		 end
		  else
			 begin
				  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
				  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - APRILE out of project duration')  
			 end
		 end
		
		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 5, 31)
		 select @Org_MAGGIO =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 5
		 if (ROUND(ISNULL(@Org_MAGGIO,0),2) <> ROUND(ISNULL(@MAGGIO,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 5) > 0 )
		 begin
			 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @MAGGIO, ELEMENT_VALUE_VL = @MAGGIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 5
		 end
	    else
		 begin
			  select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
			  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
			  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@MAGGIO,@MAGGIO)
			  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 5)
			  BEGIN
				  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@MAGGIO,@MAGGIO)
			  END
		 
		 end
		 set @updated = 1
		 end
		  else
			 begin
			  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
			  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - MAGGIO out of project duration')  
			 end
		 end
		 	
		select @AssingmentDate = DATEFROMPARTS (@ANNO, 6, 30)
		select @Org_GIUGNO =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 6
		 if (ROUND(ISNULL(@Org_GIUGNO,0),2) <> ROUND(ISNULL(@GIUGNO,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
         begin 
		  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 6) > 0 )
		 begin
				update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @GIUGNO, ELEMENT_VALUE_VL = @GIUGNO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 6
		 end
	  else
		 begin
			   select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
			  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
			  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@GIUGNO,@GIUGNO)

			  if NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 6)
			  BEGIN
				  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
			  END
		 
		 end
		 set @updated = 1
		 end
		  else
			 begin
			  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
			  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year -'  + @printYear + ' Month - GIUGNO out of project duration')  
			 end
		 end
		 	

		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 7, 31)
		 select @Org_LUGLIO=  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 7
		 if (ROUND(ISNULL(@Org_LUGLIO,0),2) <> ROUND(ISNULL(@LUGLIO,0),2))
		 begin
				if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
				begin
						   if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 7) > 0 )
							 begin
							  update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @LUGLIO, ELEMENT_VALUE_VL = @LUGLIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 7
							end
							   ELSE
								 begin
	
								 SELECT @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
								  INSERT INTO Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
								  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@LUGLIO,@LUGLIO)
									  IF NOT EXISTS(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 7)
									  BEGIN
										  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
										  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
									  END
							 end
						 set @updated = 1
				 end
					else
						begin
							insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
						values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - LUGLIO out of project duration')  
						end

	     END
	

		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 8, 31)
		 select @Org_AGOSTO =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 8
		 if (ROUND(ISNULL(@Org_AGOSTO,0),2) <> ROUND(ISNULL(@AGOSTO,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin 
		   if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 8) > 0 )
			 begin
			  update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @AGOSTO, ELEMENT_VALUE_VL = @AGOSTO where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 8
			  end
	       else
			 begin
	
				 select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
				  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@AGOSTO,@AGOSTO)

				  IF NOT EXISTS(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 8)
				  BEGIN
					  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
				  END
			 end
		 set @updated = 1
		 end

		else
		 begin
		  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
          values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - AGOSTO out of project duration')  
		 end
	end
		 
		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 9, 30)
		 select @Org_SETTEMBRE =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 9
		 if (ROUND(ISNULL(@Org_SETTEMBRE,0),2) <> ROUND(ISNULL(@SETTEMBRE,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 9) > 0 )
		 begin
		 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @SETTEMBRE, ELEMENT_VALUE_VL = @SETTEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 9
		end
		 else
			 begin
				  select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
				  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@SETTEMBRE,@SETTEMBRE)

					  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 9)
					  BEGIN
						  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
						  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
					  END
			 end
		 set @updated = 1
		 end
		  else
		 begin
		  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
          values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - SETTEMBRE out of project duration')  
		 end

		 end
		 	
		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 10, 31)
		 select @Org_OTTOBRE =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @printYear and month(STARTDATE) = 10
		 if (ROUND(ISNULL(@Org_OTTOBRE,0),2) <> ROUND(ISNULL(@OTTOBRE,0),2))
		 begin
		 if  (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 10) > 0 )
		 begin
			update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @OTTOBRE, ELEMENT_VALUE_VL = @OTTOBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 10
		 end
		  else
			 begin
				 select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
				  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
				  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@OTTOBRE,@OTTOBRE)
				  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 10)
				  BEGIN
					  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),0,0)
				  END
			 end
		 set @updated = 1
		 end
		  else
		 begin
		  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
          values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - OTTOBRE out of project duration')  
		 end

	end
		 	

		 select @AssingmentDate = DATEFROMPARTS (@ANNO, 11, 30)
		 select @Org_NOVEMBRE =  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 11
		 if (ROUND(ISNULL(@Org_NOVEMBRE,0),2) <> ROUND(ISNULL(@NOVEMBRE,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
				if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 11) > 0 )
				 begin
					 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @NOVEMBRE, ELEMENT_VALUE_VL = @NOVEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO 
					 and month(STARTDATE) = 11	 
				 end
		   else
		 begin
		
		 select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
		  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
          values(@Ass_id,@AssingmentDate,@phasingFinishDate,@NOVEMBRE,@NOVEMBRE)
		  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 11)
		  BEGIN
			  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
			  values(@Ass_id, dbo.first_day(@AssingmentDate),dbo.last_day(@phasingFinishDate),0,0)
		  END
		 end
		 set @updated = 1
		 end
		else
		 begin
		  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
          values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - NOVEMBRE out of project duration')  
		 end
		 end
		 
		  select @AssingmentDate = DATEFROMPARTS (@ANNO, 12, 31)
		 select @Org_DICEMBRE=  ELEMENT_VALUE from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 12
		 if (ROUND(ISNULL(@Org_DICEMBRE,0),2) <> ROUND(ISNULL(@DICEMBRE,0),2))
		 begin
		 if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		 begin
		  if ((select COUNT(ASS_ID) from Tab_Cost_Actual_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 12) > 0 )
		 begin
		    update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @DICEMBRE, ELEMENT_VALUE_VL = @DICEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 12
		 end
		   else
			 begin
			  select @phasingFinishDate = DATEADD(dd,-(DAY(DATEADD(mm,1,@AssingmentDate))),DATEADD(mm,1,@AssingmentDate))
			  insert into Tab_Cost_Actual_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
			  values(@Ass_id, dbo.first_day(@AssingmentDate), dbo.last_day(@phasingFinishDate),@DICEMBRE,@DICEMBRE)
				  IF NOT Exists(SELECT * from Tab_Cost_Work_Phasing where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 12)
				  BEGIN
					  insert into Tab_Cost_Work_Phasing(ASS_ID,STARTDATE,FINISHDATE,ELEMENT_VALUE,ELEMENT_VALUE_VL) 
					  values(@Ass_id,@AssingmentDate,@phasingFinishDate,0,0)
				  END
		 
			 end
		 set @updated = 1
		 end
		  else
		 begin
		  insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
          values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - DICEMBRE out of project duration')  
		 end

		 end
		 	
select * from Cost_Rev_Asso_Task
		  --end here update here assignment---	

		  if @updated = 1
		  begin
		   select @sum = sum(ISNULL(ELEMENT_VALUE,0)) from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id 

		   SET @Sumforecast=0
		   SELECT @Sumforecast= sum(ISNULL(ELEMENT_VALUE,0)) from Tab_Cost_Work_Phasing where ASS_ID =@Ass_id

		   IF @FORNITORE_CODICE is null 
		   BEGIN
		    update Cost_Rev_Asso_Task set ACTUAL_VALUE = ISNULL(@sum,0), Actual_Value_VL = ISNULL(@sum,0), ETC_VALUE =(ISNULL(@Sumforecast,0)-ISNULL(@sum,0)), ETC_Value_VL =(ISNULL(@Sumforecast,0)-ISNULL(@sum,0)),field2 = @DG_CDR where ASS_ID = @Ass_id and PROJ_ID = @c_prog and ELEMENT_ID =  @CLASSIFICAZIONE_COGE and field1 is null
		   END
		   ELSE
		   BEGIN
		    update Cost_Rev_Asso_Task set ACTUAL_VALUE = ISNULL(@sum,0), Actual_Value_VL = ISNULL(@sum,0), ETC_VALUE =(ISNULL(@Sumforecast,0)-ISNULL(@sum,0)),field2 = @DG_CDR , ETC_Value_VL =(ISNULL(@Sumforecast,0)-ISNULL(@sum,0)) where ASS_ID = @Ass_id and PROJ_ID = @c_prog and ELEMENT_ID =  @CLASSIFICAZIONE_COGE and field1 = @FORNITORE_CODICE
		   END
		  

		     insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
             values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Updated','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code -' + @PROGETTO,'')	  
		  end
        end
		else
		begin
			  --create new cost assignment---

	      set @updated = 0
	      set @taskstartdate = null
	      set @taskfinishdate = null
		  set @capexopex = null
		  set @currency = ''

		  select @currency = C_VL from PROG_T056 where C_PROG = @c_prog
		  select @capexopex =  CAPEXOPEX from CLASSE_DI_COSTO_T043 where C_CLASSE = @CLASSIFICAZIONE_COGE
          select @taskstartdate = min(TASK_START_DATE),@taskfinishdate = max(TASK_FINISH_DATE) from MSP_TASKS where PROJ_ID = @c_prog
	     
		   exec @Ass_id = PM_CreateCostAssignment 2, @CLASSIFICAZIONE_COGE,@c_prog,1,1,@capexopex,0.0,@currency,0,@taskstartdate,@taskfinishdate,@FORNITORE_CODICE,null,null,null,null
		   update Cost_Rev_Asso_Task set field1 = @FORNITORE_CODICE , field2 = @DG_CDR  where ASS_ID = @Ass_id and PROJ_ID = @c_prog


		   --added by sunny
		 --  IF(@Ass_id >0)
		 --  begin
		 --   if((select COUNT(ASS_ID) from Cost_Rev_Asso_Task where  ASS_ID = @Ass_id and field2 = @DSC_CDR and PROJ_ID = @c_prog ) > 0 )
			--begin
			-- set @updated = 1
		 --  update Cost_Rev_Asso_Task set field2 = @DG_CDR where ASS_ID = @Ass_id and PROJ_ID = @c_prog
			--end
		     
		 --  end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 1, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate) 
		   begin
		     set @updated = 1
		     update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @GENNAIO, ELEMENT_VALUE_VL = @GENNAIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 1
     
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code -n' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - GENNAIO out of project duration')  
		  end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 2, 28)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		   update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @FEBBRAIO, ELEMENT_VALUE_VL = @FEBBRAIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 2
      
          
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code -' + @CLASSIFICAZIONE_COGE + '| Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - FEBBRAIO out of project duration')  
		  end
		   

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 3, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		    update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @MARZO, ELEMENT_VALUE_VL = @MARZO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 3
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - MARZO out of project duration')  
		   end
		   
		    select @AssingmentDate = DATEFROMPARTS (@ANNO, 4, 30)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		   update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @APRILE, ELEMENT_VALUE_VL = @APRILE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 4
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - APRILE out of project duration')  
		    end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 5, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
			 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @MAGGIO, ELEMENT_VALUE_VL = @MAGGIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 5
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - MAGGIO out of project duration')  
		    end


	      select @AssingmentDate = DATEFROMPARTS (@ANNO, 6, 30)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		   update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @GIUGNO, ELEMENT_VALUE_VL = @GIUGNO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 6
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - GIUGNO out of project duration')  
		   end


		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 7, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		   update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @LUGLIO, ELEMENT_VALUE_VL = @LUGLIO where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 7
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year -' + @printYear + ' Month - LUGLIO out of project duration')  
		   end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 8, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
			 update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @AGOSTO, ELEMENT_VALUE_VL = @AGOSTO where  ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 8
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - AGOSTO out of project duration')  
		   end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 9, 30)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate )
		   begin
		    set @updated = 1
		    update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @SETTEMBRE, ELEMENT_VALUE_VL = @SETTEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO 
			and month(STARTDATE) = 9
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - SETTEMBRE out of project duration')  
		   end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 10, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		    update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @OTTOBRE, ELEMENT_VALUE_VL = @OTTOBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 10   
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + '| Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - OTTOBRE out of project duration')  
		   end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 11, 30)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		   update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @NOVEMBRE, ELEMENT_VALUE_VL = @NOVEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 11
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - NOVEMBRE out of project duration')  
		   end

		   select @AssingmentDate = DATEFROMPARTS (@ANNO, 12, 31)
		   if (@AssingmentDate >= @taskstartdate and @AssingmentDate < = @taskfinishdate)
		   begin
		    set @updated = 1
		  update Tab_Cost_Actual_Phasing set ELEMENT_VALUE = @DICEMBRE, ELEMENT_VALUE_VL = @DICEMBRE where ASS_ID = @Ass_id and year(STARTDATE) = @ANNO and month(STARTDATE) = 12
		   end
		   else
		   begin
		   insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
           values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,' year - ' + @printYear + ' Month - DICEMBRE out of project duration')  
		   end

		   if @updated = 1
		   begin

	    	SET @Sumforecast=0
		   SELECT @Sumforecast= sum(ISNULL(ELEMENT_VALUE,0)) from Tab_Cost_Work_Phasing where ASS_ID =@Ass_id

            select @sum = sum(ISNULL(ELEMENT_VALUE,0)) from Tab_Cost_Actual_Phasing where ASS_ID = @Ass_id 
		    update Cost_Rev_Asso_Task set ACTUAL_VALUE = ISNULL(@sum,0), Actual_Value_VL = ISNULL(@sum,0), field2 = @DG_CDR ,ETC_VALUE =ISNULL(@Sumforecast,0)-ISNULL(@sum,0), ETC_Value_VL =ISNULL(@Sumforecast,0)-ISNULL(@sum,0)
			where ASS_ID = @Ass_id and PROJ_ID = @c_prog and ELEMENT_ID =  @CLASSIFICAZIONE_COGE and field1 = @FORNITORE_CODICE



			 insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
             values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Inserted','Cost Code - ' + @CLASSIFICAZIONE_COGE + ' | Project Code - ' + @PROGETTO,'')	  
			  --end here create assignment---
	    end
        end
		end

		else
		begin
	    insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
        values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error',@CLASSIFICAZIONE_COGE,'Cost account code not found')
	  
		end
	  end
	  else
	  begin
	    insert into #Log(Date_,Hour_,Description_,Values_,Comment_)
        values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Error',@PROGETTO,'Project not found')
	  end


           END TRY  
          
		   BEGIN CATCH  
		    set @error = @error + 1
			
           END CATCH; 	
		      
		    SET @Inserti=@Inserti + 1
        END
-----insert codes end here ----


  insert into #Log(Date_,Hour_,Description_,Values_)
  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Number of records  updated',(select count(Values_) from #Log where Description_ = 'Updated'))

  insert into #Log(Date_,Hour_,Description_,Values_)
  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Number of records inserted',(select count(Values_) from #Log where Description_ = 'Inserted'))

  insert into #Log(Date_,Hour_,Description_,Values_)
  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'Number of records with error ',(select count(Values_) from #Log where Description_ = 'Error'))


  insert into #Log(Date_,Hour_,Description_,Values_)
  values(CONVERT(VARCHAR(8), GETUTCDATE(), 103),CONVERT(VARCHAR(8), GETUTCDATE(), 108),'End import','3 – Consuntivi Zucchetti per Progetto')
    
	  INSERT INTO ImportCostPhasing_Log(ID,[Date],[Hour],[Description],[Values],[Comment]) select @maxid ,Date_,Hour_,Description_,Values_,Comment_ from #Log order by ID_
      
	  select Date_,Hour_,Description_,Values_, Comment_ from #Log order by ID_  
	  DROP TABLE #Log
	  drop table #InsertCodes

	  EXEC dbo.Tab_ForeCast_align_by_ImportProcedure
	  EXEC dbo.Tab_Actual_align_by_ImportProcedure
	  EXEC dbo.Tab_etc_aligned_with_stored_procedure

  END
