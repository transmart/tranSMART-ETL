ALTER TABLE DEAPP.de_subject_rbm_data ADD (ID NUMBER(38,0));
create sequence deapp.de_subject_rbm_data_seq;
update DEAPP.de_subject_rbm_data set id = deapp.de_subject_rbm_data_seq.nextval where id is null;

create or replace trigger deapp.de_subject_rbm_data_id_trigger
BEFORE INSERT ON deapp.de_subject_rbm_data 
FOR EACH ROW

BEGIN
  SELECT deapp.de_subject_rbm_data_seq.NEXTVAL
  INTO   :new.id
  FROM   dual;
END;
/

ALTER TABLE DEAPP.de_subject_rbm_data ADD CONSTRAINT pk_de_subject_rbm_data PRIMARY KEY ("ID");

CREATE TABLE DEAPP.DE_RBM_DATA_ANNOTATION_JOIN 
(
  DATA_ID NUMBER(38,0),
  ANNOTATION_ID NUMBER(38,0),
  FOREIGN KEY (DATA_ID) REFERENCES DEAPP.DE_SUBJECT_RBM_DATA ("ID") ON DELETE CASCADE,
  FOREIGN KEY (ANNOTATION_ID) REFERENCES DEAPP.DE_RBM_ANNOTATION ("ID") ON DELETE CASCADE,
  CONSTRAINT PK_DE_RBM_DATA_ANNOTATION_JOIN PRIMARY KEY(DATA_ID, ANNOTATION_ID) 
);
 
insert into DEAPP.DE_RBM_DATA_ANNOTATION_JOIN
select d.id, ann.id from deapp.de_subject_rbm_data d
inner join deapp.de_rbm_annotation ann on ann.antigen_name = d.antigen_name
inner join deapp.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id and ann.gpl_id = ssm.gpl_id
where not exists( select * from deapp.de_rbm_data_annotation_join j where j.data_id = d.id AND j.annotation_id = ann.id );
