/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/
  

package com.recomdata.pipeline.transmart

import groovy.sql.Sql;

import org.apache.log4j.Logger;

class BioAssayAnalysisData {

	private static final Logger log = Logger.getLogger(BioAssayAnalysisData)

	Sql biomart
	String testsDataTable
	long bioAssay

	void loadBioAssayAnalysisData(){

		log.info "Start loading data into BIO_ASSAY_ANALYSIS_DATA ..."

		String qry = """ insert /*+ parallel (BIO_ASSAY_ANALYSIS_DATA, 8) */
						into BIO_ASSAY_ANALYSIS_DATA
						nologging
						(
						  BIO_ASY_ANALYSIS_DATA_ID,
						  FOLD_CHANGE_RATIO,
						  RAW_PVALUE,
						  ADJUSTED_PVALUE,
						  BIO_ASSAY_ANALYSIS_ID ,
						  FEATURE_GROUP_NAME,
						  BIO_ASSAY_FEATURE_GROUP_ID,
						  BIO_EXPERIMENT_ID,
						  BIO_ASSAY_PLATFORM_ID,
						  PREFERRED_PVALUE,
						  TEA_NORMALIZED_PVALUE
						 )
						select
						  BIO_ASY_ANALYSIS_DATA_ID,
						  FOLD_CHANGE_RATIO,
						  RAW_PVALUE,
						  ADJUSTED_PVALUE,
						  BIO_ASSAY_ANALYSIS_ID ,
						  FEATURE_GROUP_NAME,
						  BIO_ASSAY_FEATURE_GROUP_ID,
						  BIO_EXPERIMENT_ID,
						  BIO_ASSAY_PLATFORM_ID,
						  PREFERRED_PVALUE,
						  TEA_NORMALIZED_PVALUE
						from ASSAY_ANALYSIS_DATA
						where (fold_change_ratio >=1.0 or fold_change_ratio<=-1.0) and
						      (preferred_pvalue is null or preferred_pvalue <=0.1)
					 """

		biomart.execute(qry)

		log.info "End loading data into BIO_ASSAY_ANALYSIS_DATA ..."
	}


	void loadBioAssayAnalysisData(long bioDataId, String uniqueId, String dataType){

		if(isBioAssayAnalysisDataExist(bioDataId, uniqueId, dataType)){
			log.info "($bioDataId, $uniqueId, $dataType) already exists in BIO_ASSAY_ANALYSIS_DATA ..."
		}else{
			log.info "Start loading ($bioDataId, $uniqueId, $dataType) into BIO_ASSAY_ANALYSIS_DATA ..."

			String qry = """ insert into bio_assay_analysis_data(bio_data_id, unique_id, bio_data_type) values(?, ?, ?) """
			biomart.execute(qry, [
				bioDataId,
				uniqueId,
				dataType
			])

			log.info "End loading ($bioDataId, $uniqueId, $dataType) into BIO_ASSAY_ANALYSIS_DATA ..."
		}
	}


	boolean isBioAssayAnalysisDataExist(long bioExperimentId){
		String qry = "select count(1) from bio_assay_analysis_data where bio_experimentt_id=?"
		if(biomart.firstRow(qry, [bioExperimentId,])[0] > 0){
			return true
		}else{
			return false
		}
	}


	void dropBioAssayAnalysisDataIndexes(){
		log.info "Start dropping indexes for BIO_ASSAY_ANALYSIS_DATA ... "
		
		String stmt = ""
		String qry = """ select index_name from user_indexes 
					     where table_name='BIO_ASSAY_ANALYSIS_DATA' 
							   and index_name not like 'PK_%' and index_name not like '%_PK'"""
		biomart.eachRow(qry) {
			stmt = "drop index ${it.index_name}"
			biomart.execute(stmt)
		}
		
		log.info "End dropping indexes for BIO_ASSAY_ANALYSIS_DATA ... "
	}


	void createBioAssayAnalysisDataIndexes(){
		
		log.info "End recreating indexes for BIO_ASSAY_ANALYSIS_DATA ... "
		
		String qry = ""
		qry = """ create index idx_baad_probe on BIO_ASSAY_ANALYSIS_DATA (FEATURE_GROUP_NAME)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_platform on BIO_ASSAY_ANALYSIS_DATA (BIO_ASSAY_PLATFORM_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_analysis on BIO_ASSAY_ANALYSIS_DATA 
						(BIO_ASSAY_ANALYSIS_ID, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_analysis_probe on BIO_ASSAY_ANALYSIS_DATA 
						(BIO_ASSAY_ANALYSIS_ID, BIO_ASSAY_FEATURE_GROUP_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_experiment_analysis on BIO_ASSAY_ANALYSIS_DATA 
						(BIO_EXPERIMENT_ID, BIO_ASSAY_ANALYSIS_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_probe1 on BIO_ASSAY_ANALYSIS_DATA 
						(FEATURE_GROUP_NAME, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_eap on BIO_ASSAY_ANALYSIS_DATA 
						(BIO_EXPERIMENT_ID, BIO_ASSAY_ANALYSIS_ID, BIO_ASSAY_FEATURE_GROUP_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_baad_experiment_analysis1 on BIO_ASSAY_ANALYSIS_DATA 
						(BIO_EXPERIMENT_ID, BIO_ASSAY_ANALYSIS_ID, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		log.info "Start recreating indexes for BIO_ASSAY_ANALYSIS_DATA ... "
	}
	
	
	void disableBioAssayAnalysisDataConstraints(){
		
		log.info "Start disabling constraints for BIO_ASSAY_ANALYSIS_DATA ... "
		
		String stmt = ""
		String qry = """ select constraint_name from user_constraints
						 where table_name='BIO_ASSAY_ANALYSIS_DATA' and constraint_type='R' """
		biomart.eachRow(qry) {
			stmt = "alter table BIO_ASSAY_ANALYSIS_DATA disable constraint ${it.constraint_name}"
			biomart.execute(stmt)
		}
		
		log.info "Start disabling constraints for BIO_ASSAY_ANALYSIS_DATA ... "
	}
	
	
	void enableBioAssayAnalysisDataConstraints(){
		
		log.info "Start enabling constraints for BIO_ASSAY_ANALYSIS_DATA ... "
		
		String stmt = ""
		String qry = """ select constraint_name from user_constraints
						 where table_name='BIO_ASSAY_ANALYSIS_DATA' and constraint_type='R' """
		biomart.eachRow(qry) {
			stmt = "alter table BIO_ASSAY_ANALYSIS_DATA enable constraint ${it.constraint_name}"
			biomart.execute(stmt)
		}
		
		log.info "Start enabling constraints for BIO_ASSAY_ANALYSIS_DATA ... "
	}

	
	void setTestsDataTable(String testsDataTable){
		this.testsDataTable = testsDataTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
