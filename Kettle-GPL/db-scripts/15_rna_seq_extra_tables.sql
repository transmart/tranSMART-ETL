

	create table "DEAPP"."DE_RNASEQ_ANNOTATION"
	(
  	GPL_ID varchar2(50),
  	TRANSCRIPT_ID varchar2(50),
  	GENE_SYMBOL varchar2(50),
  	GENE_ID varchar2(50),
  	ORGANISM varchar2(30),
	PROBESET_ID NUMBER(38,0)
	)


	create or replace synonym tm_cz.DE_RNASEQ_ANNOTATION for "DEAPP"."DE_RNASEQ_ANNOTATION"  ;

	grant select, insert, update, delete on "DEAPP"."DE_RNASEQ_ANNOTATION" to tm_cz;

------------------------------------------------------------------------------------------------------------


	create table "TM_LZ"."LT_RNASEQ_ANNOTATION"
	(
	  TRANSCRIPT_ID varchar2(50),
 	 GENE_SYMBOL varchar2(50),
 	 ORGANISM varchar2(30)
	)

	create or replace synonym tm_cz.LT_RNASEQ_ANNOTATION for "TM_LZ"."LT_RNASEQ_ANNOTATION"  ;

	grant select, insert, update, delete on "TM_LZ"."LT_RNASEQ_ANNOTATION" to tm_cz;


---------------------------------------------------------------------------------------------------------


--insert into de_gpl_info table
--insert into de_gpl_info(platform,annotation_title,marker_type,organism) values('RBM200','RNASEQ','RNASEQ','Homo sapiens')
--note the insert command value can be change according to the sample file
