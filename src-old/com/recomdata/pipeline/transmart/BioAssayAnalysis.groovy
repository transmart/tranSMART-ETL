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

class BioAssayAnalysis {

	private static final Logger log = Logger.getLogger(BioAssayAnalysis)

	Sql biomart
	long bioAssayAnalysisPlatformId

	/**
	 * Insert a single analysis into BIO_ASSAY_ANALYSIS table
	 * 
	 * @param analysisName
	 * @param gseName
	 */
	void loadBioAssayAnalysis(String analysisName, String studyName){

		String qry = """ INSERT INTO BIO_ASSAY_ANALYSIS (ANALYSIS_NAME, QA_CRITERIA, 
								BIO_ASY_ANALYSIS_PLTFM_ID, SHORT_DESCRIPTION, LONG_DESCRIPTION, 
								ANALYSIS_METHOD_CD, BIO_ASSAY_DATA_TYPE, ETL_ID, ANALYSIS_CREATE_DATE) 
						 VALUES(?, ?, ?, ?, ?, 'comparison', 'Gene Expression', ?, sysdate) """

		if(isBioAssayAnalysisExist(analysisName, studyName)){
			log.info "$analysisName($studyName) already exists in BIO_ASSAY_ANALYSIS ..."
		}else{
			log.info "Start inserting $analysisName($studyName) into BIO_ASSAY_ANALYSIS ..."

			biomart.execute(qry, [
				analysisName,
				analysisName,
				bioAssayAnalysisPlatformId,
				analysisName,
				analysisName,
				studyName
			])

			log.info "End inserting $analysisName($studyName) into BIO_ASY_ANALYSIS_PLTFM ..."
		}
	}

	
	void updateTeaDataCount(long bioAssayAnalysisId, int teaDataCount){
		String qry = " update bio_assay_analysis set tea_data_count=? where bio_assay_analysis_id=? "
		biomart.execute(qry, [teaDataCount, bioAssayAnalysisId])
	}

	
	void updateDataCount(long bioAssayAnalysisId, int dataCount){
		String qry = "update bio_assay_analysis set data_count=? where bio_assay_analysis_id=?"
		biomart.execute(qry, [dataCount, bioAssayAnalysisId])
	}

	
	boolean isBioAssayAnalysisExist(String analysisName, String studyName){

		String qry = """ select count(1) from bio_assay_analysis 
                         where analysis_name=? and etl_id=? """
		if(biomart.firstRow(qry, [
			analysisName,
			studyName
		])[0] > 0){
			return true
		}else{
			return false
		}
	}
	

	void setBioAssayAnalysisPlatformId(long bioAssayAnalysisPlatformId){
		this.bioAssayAnalysisPlatformId = bioAssayAnalysisPlatformId
	}
	

	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
