CREATE SEQUENCE  "DEAPP"."RBM_ANNOTATION_ID"  MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 9255 NOCACHE  NOORDER  NOCYCLE ;



CREATE OR REPLACE TRIGGER "DEAPP"."RBM_ID_TRIGGER" 

  before insert on "DEAPP"."DE_RBM_ANNOTATION" 

  for each row 

begin  

  if inserting then 

      if :NEW."ID" is null then 

        select RBM_ANNOTATION_ID.nextval into :NEW."ID" from dual; 

      end if; 

  end if; 

end;

/

ALTER TRIGGER "DEAPP"."RBM_ID_TRIGGER" ENABLE;
