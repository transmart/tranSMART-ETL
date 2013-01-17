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

class BioDataUid {

	private static final Logger log = Logger.getLogger(BioDataUid)

	Sql biomart

	void loadBioDataUid(){
		loadExperimentBioDataUid()
		loadAnalysisBioDataUid()
		loadDiseaseBioDataUid()
	}


	void loadExperimentBioDataUid(){

		log.info "Start inserting BIO_DATA_UID for BIO_EXPERIMENT ... "

		String qry = """ INSERT INTO BIO_DATA_UID(BIO_DATA_ID, UNIQUE_ID, BIO_DATA_TYPE)
						 select bio_experiment_id, 'Omicsoft: '||accession, 'EXP'
						 from bio_experiment 
						 where bio_experiment_id not in (select bio_data_id from bio_data_uid)
					 """
		biomart.execute(qry)

		log.info "Stop inserting BIO_DATA_UID for BIO_EXPERIMENT ... "
	}


	void loadAnalysisBioDataUid(){

		log.info "Start inserting BIO_DATA_UID for BIO_ASSAY_ANALYSIS ... "

		String qry = """ INSERT INTO BIO_DATA_UID(BIO_DATA_ID, UNIQUE_ID, BIO_DATA_TYPE)
						 SELECT bio_assay_analysis_id, etl_id||':'||analysis_name, 'BAA'
						 from bio_assay_analysis
						 where bio_assay_analysis_id not in (select bio_data_id from bio_data_uid)
					 """
		biomart.execute(qry)

		log.info "Stop inserting into BIO_DATA_UID for BIO_ASSAY_ANALYSIS ... "
	}

	
	void loadDiseaseBioDataUid(){

		log.info "Start inserting BIO_DATA_UID for BIO_DISEASE ... "

		String qry = """ insert into bio_data_uid (bio_data_id, unique_id, bio_data_type)
					     select BIO_DISEASE_ID, 'DIS:'||MESH_CODE, 'BIO_DISEASE'
						 from bio_disease
						 where BIO_DISEASE_ID not in (select bio_data_id from bio_data_uid)
					"""
		biomart.execute(qry)

		log.info "Stop inserting BIO_DATA_UID for BIO_DISEASE ... "
	}

	
	void loadBioDataUid(long bioDataId, String uniqueId, String dataType){

		if(isBioDataUidExist(bioDataId, uniqueId, dataType)){
			log.info "($bioDataId, $uniqueId, $dataType) already exists in BIO_DATA_UID ..."
		}else{
			log.info "Start loading ($bioDataId, $uniqueId, $dataType) into BIO_DATA_UID ..."

			String qry = """ insert into bio_data_uid(bio_data_id, unique_id, bio_data_type) values(?, ?, ?) """
			biomart.execute(qry, [
				bioDataId,
				uniqueId,
				dataType
			])

			log.info "End loading ($bioDataId, $uniqueId, $dataType) into BIO_DATA_UID ..."
		}
	}


	boolean isBioDataUidExist(long bioDataId, String uniqueId, String dataType){
		String qry = "select count(1) from bio_data_uid where bio_data_id=? and unique_id=? and data_type=?"
		if(biomart.firstRow(qry, [
			bioDataId,
			uniqueId,
			dataType
		])[0] > 0){
			return true
		}else{
			return false
		}
	}

	
	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
