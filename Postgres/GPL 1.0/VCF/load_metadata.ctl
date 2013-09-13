-- Please note, the metadata comments are not loaded from a file in this postgres version
-- as postgres doesn't have a method for that
COPY DEAPP.DE_VARIANT_DATASET 
  ( DATASET_ID, DATASOURCE_ID, ETL_ID, ETL_DATE, GENOME, METADATA_COMMENT )
    FROM './load_metadata.txt' DELIMITER E'\t';
