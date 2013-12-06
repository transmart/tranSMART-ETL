--
-- Proteomics annotation
create table deapp.de_protein_annotation(
 id number(*,0) primary key,
 gpl_id varchar2(50 byte) not null, --same as deapp.de_gpl_info.platform
 peptide varchar2(200 char) not null,
 uniprot_id varchar2(50 char),
 biomarker_id nvarchar2(200), --same as biomart.bio_marker.primary_external_id
 organism varchar2(200 char)
);
--
comment on column deapp.de_protein_annotation.gpl_id is
'references platform on de_gpl_info';
comment on column deapp.de_protein_annotation.peptide is
'peptide sequence';
comment on column deapp.de_protein_annotation.uniprot_id is
'UniProtID for this analyte. Refers to the uniprot database (www.uniprot.org), which is loaded as a dictionary in the biomarker table.';
--
alter table deapp.de_subject_protein_data
add (protein_annotation_id number(*,0) references deapp.de_protein_annotation);
 
-- Changes to the proteomics table to handle other data
alter table deapp.de_subject_protein_data modify ( component VARCHAR2(200 byte) );
alter table deapp.de_subject_protein_data add ( log_intensity NUMBER );
