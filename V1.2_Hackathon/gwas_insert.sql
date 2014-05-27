insert into tm_lz.lz_src_analysis_metadata
(study_id
, data_type
, analysis_name
, description
, phenotype_ids
,population
,tissue
,genome_version
,genotype_platform_ids
,expression_platform_ids
,statistical_test
,research_unit
,sample_size
,cell_type
,pvalue_cutoff
,filename
,status
,etl_id
,analysis_name_archived
,model_name
,model_desc
,sensitive_flag
,sensitive_desc)
select 'GSE26554'
,'GWAS'
,'Test 1'
,'First test'
,'MESH:D001171'
,'population'
,'tissue'
,null
,null
,'GPL570'
,'stat test'
,'research unit'
,'sample size'
,'cell type'
,'.5'
,'C:\Users\javitabile\Documents\hackathon\Data\GSE26554_GWAS.txt'
,'PENDING'
,nextval('tm_lz.seq_etl_id')
,'analysis name archived'
,'model name'
,'model desc'
,0
,'sensitive desc';