CREATE TABLE "TM_LZ"."LT_QPCR_MIRNA_ANNOTATION" 
   (	"ID_REF" VARCHAR2(100 BYTE), 
	"MIRNA_ID" VARCHAR2(100 BYTE), 
	"SN_ID" VARCHAR2(100 BYTE), 
	"ORGANISM" VARCHAR2(1000 BYTE)
   )
   
   create or replace synonym tm_cz.LT_QPCR_MIRNA_ANNOTATION for "TM_LZ"."LT_QPCR_MIRNA_ANNOTATION" ;
   grant select, insert, update, delete on  "TM_LZ"."LT_QPCR_MIRNA_ANNOTATION" to tm_cz;

   ------------------------------------------------------



CREATE TABLE "TM_LZ"."LT_SRC_MIRNA_DEAPP_ANNOT" 
   (	"ID_REF" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"MIRNA_SYMBOL" VARCHAR2(100 BYTE), 
	"MIRNA_ID" VARCHAR2(250 BYTE), 
	"ORGANISM" VARCHAR2(200 BYTE)
   )
  
   create or replace synonym tm_cz.LT_SRC_MIRNA_DEAPP_ANNOT for "TM_LZ"."LT_SRC_MIRNA_DEAPP_ANNOT" ;
   grant select, insert, update, delete on  "TM_LZ"."LT_SRC_MIRNA_DEAPP_ANNOT" to tm_cz;

   
   --------------------------------------------------------------

   CREATE TABLE "TM_CZ"."MIRNA_ANNOTATION_DEAPP" 
   (	"ID_REF" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"MIRNA_SYMBOL" VARCHAR2(100 BYTE), 
	"MIRNA_ID" VARCHAR2(100 BYTE), 
	"PROBESET_ID" NUMBER(38,0), 
	"ORGANISM" VARCHAR2(200 BYTE)
   )


   --------------------------------------------------------------
   CREATE TABLE "TM_CZ"."MIRNA_PROBESET_DEAPP" 
   (	"PROBESET_ID" NUMBER(38,0) NOT NULL ENABLE, 
	"PROBESET" VARCHAR2(100 BYTE), 
	"PLATFORM" VARCHAR2(100 BYTE), 
	"ORGANISM" VARCHAR2(200 BYTE)
   )
    CREATE OR REPLACE TRIGGER "TM_CZ"."TRG_MIRNA_PROBESET_DEAPP" 
    before insert on "MIRNA_PROBESET_DEAPP"    
	for each row begin     
		if inserting then       
			if :NEW."PROBESET_ID" is null then
				select SEQ_PROBESET_ID.nextval into :NEW."PROBESET_ID" from dual;       
			end if;   
		end if; 
	end;

   --------------------------------------------------------------


    CREATE TABLE "BIOMART"."MIRNA_BIO_MARKER" 
   (	
	"BIO_MARKER_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"BIO_MARKER_NAME" NVARCHAR2(200), 
	"BIO_MARKER_DESCRIPTION" NVARCHAR2(1000), 
	"ORGANISM" NVARCHAR2(200), 
	"PRIMARY_SOURCE_CODE" NVARCHAR2(200), 
	"PRIMARY_EXTERNAL_ID" NVARCHAR2(200), 
	"BIO_MARKER_TYPE" NVARCHAR2(200) NOT NULL ENABLE, 
	 UNIQUE ("ORGANISM", "PRIMARY_EXTERNAL_ID")
      )
         
	create or replace synonym tm_cz.MIRNA_BIO_MARKER for "BIOMART"."MIRNA_BIO_MARKER" ;
	grant select, insert, update, delete on  "BIOMART"."MIRNA_BIO_MARKER" to tm_cz;
--------------------------------------------------
         
         CREATE TABLE "BIOMART"."MIRNA_BIO_ASSAY_FEATURE_GROUP" 
      (  "BIO_ASSAY_FEATURE_GROUP_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"FEATURE_GROUP_NAME" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"FEATURE_GROUP_TYPE" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	 CONSTRAINT "MIRNA_BIO_ASY_FEATURE_GRP_PK" PRIMARY KEY ("BIO_ASSAY_FEATURE_GROUP_ID")
         
       )
	  CREATE OR REPLACE TRIGGER "BIOMART"."TRG_MIRNA_BIO_ASSAY_F_G_ID" before insert on "BIOMART"."MIRNA_BIO_ASSAY_FEATURE_GROUP"  
          for each row begin 
	  if inserting then  
	  if :NEW."BIO_ASSAY_FEATURE_GROUP_ID" is null then   
	  select SEQ_BIO_DATA_ID.nextval into :NEW."BIO_ASSAY_FEATURE_GROUP_ID" from dual;
	  end if;
	  end if; end;

	 create or replace synonym tm_cz.MIRNA_BIO_ASSAY_FEATURE_GROUP for "BIOMART"."MIRNA_BIO_ASSAY_FEATURE_GROUP" ;
         grant select, insert, update, delete on  "BIOMART"."MIRNA_BIO_ASSAY_FEATURE_GROUP" to tm_cz;

   -----------------------------------------------------------------------


         
         CREATE TABLE "BIOMART"."MIRNA_BIO_ASSAY_DATA_ANNOT" 
     (	"BIO_ASSAY_FEATURE_GROUP_ID" NUMBER(18,0), 
	"BIO_MARKER_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"DATA_TABLE" CHAR(5 BYTE)
     )
     

     create or replace synonym tm_cz.MIRNA_BIO_ASSAY_DATA_ANNOT for "BIOMART"."MIRNA_BIO_ASSAY_DATA_ANNOT" ;
     grant select, insert, update, delete on  "BIOMART"."MIRNA_BIO_ASSAY_DATA_ANNOT" to tm_cz;


   ---------------------------------------------------------
    CREATE TABLE "DEAPP"."DE_QPCR_MIRNA_ANNOTATION" 
   (	"ID_REF" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"MIRNA_SYMBOL" VARCHAR2(100 BYTE), 
	"MIRNA_ID" VARCHAR2(100 BYTE), 
	"PROBESET_ID" NUMBER(38,0),
	"ORGANISM" VARCHAR2(200 BYTE)
   )


   create or replace synonym tm_cz.DE_QPCR_MIRNA_ANNOTATION for "DEAPP"."DE_QPCR_MIRNA_ANNOTATION" ;
   grant select, insert, update, delete on  "DEAPP"."DE_QPCR_MIRNA_ANNOTATION" to tm_cz;

   -----------------------------------------------------------

   begin
   util_grant_all('TM_CZ','TABLES');
   end;