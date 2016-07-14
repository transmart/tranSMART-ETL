CREATE TABLE searchapp.import_xnat_configuration
(
  id bigint NOT NULL,
  version bigint NOT NULL,
  name character varying(255) NOT NULL,
  description text,
  url character varying(255) NOT NULL,
  username character varying(255) NOT NULL,
  project character varying(255) NOT NULL,
  node character varying(255) NOT NULL,
  CONSTRAINT pk_import_xnat_configuration PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE searchapp.import_xnat_configuration
  OWNER TO searchapp;
GRANT ALL ON TABLE searchapp.import_xnat_configuration TO searchapp;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE searchapp.import_xnat_configuration TO biomart_user;
GRANT ALL ON TABLE searchapp.import_xnat_configuration TO tm_cz;

CREATE TABLE searchapp.import_xnat_variable
(
  id bigint NOT NULL,
  configuration_id bigint NOT NULL,
  name character varying(255) NOT NULL,
  datatype character varying(255) NOT NULL,
  url character varying(255) NOT NULL,
  CONSTRAINT pk_import_xnat_variable PRIMARY KEY (id),
  CONSTRAINT fk_import_xnat_var_to_config FOREIGN KEY (configuration_id)
      REFERENCES searchapp.import_xnat_configuration (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE searchapp.import_xnat_variable
  OWNER TO searchapp;
GRANT ALL ON TABLE searchapp.import_xnat_variable TO searchapp;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE searchapp.import_xnat_variable TO biomart_user;
GRANT ALL ON TABLE searchapp.import_xnat_variable TO tm_cz;
