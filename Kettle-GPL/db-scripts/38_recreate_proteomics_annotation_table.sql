drop table "TM_LZ"."LT_PROTEIN_ANNOTATION";
create table "TM_LZ"."LT_PROTEIN_ANNOTATION"
(
GPL_ID varchar2(100),
PEPTIDE varchar2(200),
UNIPROT_ID varchar2(200),
BIOMARKER_ID number,
ORGANISM varchar2(100)

);

create or replace synonym tm_cz.LT_PROTEIN_ANNOTATION for "TM_LZ"."LT_PROTEIN_ANNOTATION" ;
grant select, insert, update, delete on "TM_LZ"."LT_PROTEIN_ANNOTATION" to tm_cz;