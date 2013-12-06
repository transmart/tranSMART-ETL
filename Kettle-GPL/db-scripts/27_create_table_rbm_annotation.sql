 CREATE TABLE "TM_CZ"."ANTIGEN_DEAPP" 
   (	"ANTIGEN_ID" NUMBER(38,0) NOT NULL ENABLE, 
	"ANTIGEN_NAME" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"PLATFORM" VARCHAR2(100 BYTE) NOT NULL ENABLE 
   )

	CREATE SEQUENCE  "TM_CZ"."SEQ_ANTIGEN_ID"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE ;
 
	CREATE OR REPLACE TRIGGER "TM_CZ"."TRG_ANTIGEN_DEAPP" 
	before insert on "ANTIGEN_DEAPP"    
	for each row begin     
		if inserting then       
			if :NEW."ANTIGEN_ID" is null then
				select SEQ_ANTIGEN_ID.nextval into :NEW."ANTIGEN_ID" from dual;       
			end if;   
		end if; 
	end;

	ALTER TRIGGER "TM_CZ"."TRG_ANTIGEN_DEAPP" ENABLE;


	--------------------------------------------------------------------------------------
  CREATE TABLE "TM_LZ"."LT_SRC_RBM_ANNOTATION" 
   (	"GPL_ID" VARCHAR2(50 BYTE), 
	"ANTIGEN_NAME" VARCHAR2(200 BYTE), 
	"UNIPROTID" VARCHAR2(50 BYTE), 
	"GENE_SYMBOL" VARCHAR2(200 BYTE), 
	"GENE_ID" VARCHAR2(50 BYTE)
   )

	create or replace synonym tm_cz.LT_SRC_RBM_ANNOTATION for ""TM_LZ"."LT_SRC_RBM_ANNOTATION"   ;
	grant select, insert, update, delete on "TM_LZ"."LT_SRC_RBM_ANNOTATION"  to tm_cz;

----------------------------------------------------------------

