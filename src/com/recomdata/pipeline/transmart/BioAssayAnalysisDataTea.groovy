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

class BioAssayAnalysisDataTea {

	private static final Logger log = Logger.getLogger(BioAssayAnalysisDataTea)

	Sql biomart
	String testsDataTable
	long bioAssay

	void loadBioAssayAnalysisDataTea(){

		log.info "Start loading data into BIO_ASSAY_ANALYSIS_DATA_TEA ..."

		String qry = """ insert /*+ parallel (BIO_ASSAY_ANALYSIS_DATA_TEA, 8) */
						into BIO_ASSAY_ANALYSIS_DATA_TEA
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
						  TEA_NORMALIZED_PVALUE,
						  BIO_EXPERIMENT_TYPE)
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
						  TEA_NORMALIZED_PVALUE,
						  'Experiment'
						from ASSAY_ANALYSIS_DATA
						where TEA_NORMALIZED_PVALUE <= 0.05
					 """

		biomart.execute(qry)

		log.info "End loading data into BIO_ASSAY_ANALYSIS_DATA_TEA ..."
	}



	boolean isBioAssayAnalysisDataTeaExist(long bioExperimentId){
		String qry = "select count(1) from bio_assay_analysis_data_tea where bio_experimentt_id=?"
		if(biomart.firstRow(qry, [bioExperimentId,])[0] > 0){
			return true
		}else{
			return false
		}
	}


	void dropBioAssayAnalysisDataTeaIndexes(){
		log.info "Start dropping indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ... "

		String stmt = ""
		String qry = """ select index_name from user_indexes
						 where table_name='BIO_ASSAY_ANALYSIS_DATA_TEA'
							   and index_name not like 'PK_%' and index_name not like '%_PK'"""
		biomart.eachRow(qry) {
			stmt = "drop index ${it.index_name}"
			biomart.execute(stmt)
		}

		log.info "End dropping indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ... "
	}


	void createBioAssayAnalysisDataTeaIndexes(){

		log.info "End recreating indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ... "

		String qry = ""
		qry = """ create index idx_tea_probe_name on BIO_ASSAY_ANALYSIS_DATA_TEA 
					(FEATURE_GROUP_NAME, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_tea_probe_id on BIO_ASSAY_ANALYSIS_DATA_TEA
					(BIO_ASSAY_FEATURE_GROUP_ID, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_tea_experiment_type on BIO_ASSAY_ANALYSIS_DATA_TEA 
					(BIO_EXPERIMENT_TYPE, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_tea_experiment_analysis on BIO_ASSAY_ANALYSIS_DATA_TEA
					(BIO_EXPERIMENT_ID, BIO_ASSAY_ANALYSIS_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_tea_experiment_analysis1 on BIO_ASSAY_ANALYSIS_DATA_TEA
					(BIO_EXPERIMENT_ID, BIO_ASSAY_ANALYSIS_ID, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		qry = """ create index idx_tea_analysis on BIO_ASSAY_ANALYSIS_DATA_TEA
						(BIO_ASSAY_ANALYSIS_ID, BIO_ASY_ANALYSIS_DATA_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)
		
		qry = """ create index idx_tea_probe_analysis on BIO_ASSAY_ANALYSIS_DATA_TEA
						(BIO_ASSAY_FEATURE_GROUP_ID, BIO_ASSAY_ANALYSIS_ID)
				  TABLESPACE INDX NOLOGGING PARALLEL 8 """
		biomart.execute(qry)

		log.info "Start recreating indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ... "
	}


	void disableBioAssayAnalysisDataTeaConstraints(){

		log.info "Start disabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ... "

		String stmt = ""
		String qry = """ select constraint_name from user_constraints
						 where table_name='BIO_ASSAY_ANALYSIS_DATA_TEA' and constraint_type='R' """
		biomart.eachRow(qry) {
			stmt = "alter table BIO_ASSAY_ANALYSIS_DATA_TEA disable constraint ${it.constraint_name}"
			biomart.execute(stmt)
		}

		log.info "Start disabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ... "
	}


	void enableBioAssayAnalysisDataTeaConstraints(){

		log.info "Start enabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ... "

		String stmt = ""
		String qry = """ select constraint_name from user_constraints
						 where table_name='BIO_ASSAY_ANALYSIS_DATA_TEA' and constraint_type='R' """
		biomart.eachRow(qry) {
			stmt = "alter table BIO_ASSAY_ANALYSIS_DATA_TEA enable constraint ${it.constraint_name}"
			biomart.execute(stmt)
		}

		log.info "Start enabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ... "
	}


	void setTestsDataTable(String testsDataTable){
		this.testsDataTable = testsDataTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
