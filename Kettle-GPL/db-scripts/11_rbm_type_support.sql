alter table deapp.de_subject_rbm_data
add (unit varchar2(50 char));
--
comment on column deapp.de_subject_rbm_data.unit is
'unit of the numeric value stored in the VALUE column';
--
create table deapp.de_rbm_annotation(
 id number(*,0) primary key,
 gpl_id varchar2(50 byte) not null, --same as deapp.de_gpl_info.platform
 antigen_name varchar2(200 char) not null,
 uniprot_id varchar2(50 char),
 gene_symbol varchar2(50 char),
 gene_id nvarchar2(200) --same as biomart.bio_marker.primary_external_id
);
--
comment on column deapp.de_rbm_annotation.gpl_id is
'references platform on de_gpl_info';
comment on column deapp.de_rbm_annotation.antigen_name is
'name of the analyte';
comment on column deapp.de_rbm_annotation.uniprot_id is
'UniProtID for this analyte. Refers to the uniprot database (www.uniprot.org), which is loaded as a dictionary in the biomarker table.';
comment on column deapp.de_rbm_annotation.gene_symbol is
'Gene symbol associated with this antigen, if any';
comment on column deapp.de_rbm_annotation.gene_id is
'Gene id associated with this antigen, if any. Refers to primary_external_id in a record in biomart_bio_marker';
--
alter table deapp.de_subject_rbm_data
add (rbm_annotation_id number(*,0) references deapp.de_rbm_annotation);
