alter table tm_wz.WT_SUBJECT_MIRNA_PROBESET modify probeset_id NUMBER(38,0);


alter table deapp.DE_SUBJECT_MIRNA_DATA add probeset_id_num NUMBER(38,0);
update deapp.DE_SUBJECT_MIRNA_DATA set probeset_id_num = TO_NUMBER(probeset_id) WHERE LENGTH(TRIM(TRANSLATE(probeset_id, ' 0123456789',' '))) IS NULL;
alter table deapp.DE_SUBJECT_MIRNA_DATA drop column probeset_id;
alter table deapp.DE_SUBJECT_MIRNA_DATA rename column probeset_id_num to probeset_id;