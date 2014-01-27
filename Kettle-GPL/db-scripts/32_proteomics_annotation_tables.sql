create table "TM_LZ"."LT_PROTEIN_ANNOTATION"
(
GPL_ID varchar2(20),
PEPTIDE varchar2(50),
UNIPROTID varchar2(30),
BIOMARKER_ID number,
ORGANISM varchar2(30)

);

create or replace synonym tm_cz.LT_PROTEIN_ANNOTATION for "TM_LZ"."LT_PROTEIN_ANNOTATION" ;
grant select, insert, update, delete on "TM_LZ"."LT_PROTEIN_ANNOTATION" to tm_cz;
grant select, insert, update, delete on "DEAPP"."DE_PROTEIN_ANNOTATION" to tm_cz;