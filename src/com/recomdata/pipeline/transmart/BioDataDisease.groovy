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

class BioDataDisease {

	private static final Logger log = Logger.getLogger(BioDataDisease)

	Sql biomart

	void loadBioDataDisease(long bioDataId, long bioDiseaseId, String etlSource){

		if(isBioDataDiseaseExist(bioDataId, bioDiseaseId, etlSource)){
			log.info "($bioDataId, $bioDiseaseId, $etlSource) already exists in BIO_DATA_DISEASE ..."
		}else{
			log.info "Start loading ($bioDataId, $bioDiseaseId, $etlSource) into BIO_DATA_DISEASE ..."

			String qry = """ insert into bio_data_disease(bio_data_id, bio_disease_id, etl_source) values(?, ?, ?) """
			biomart.execute(qry, [
				bioDataId,
				bioDiseaseId,
				etlSource
			])

			log.info "End loading ($bioDataId, $bioDiseaseId, $etlSource) into BIO_DATA_DISEASE ..."
		}
	}


	boolean isBioDataDiseaseExist(long bioDataId, long bioDiseaseId, String etlSource){
		String qry = "select count(1) from bio_data_disease where bio_data_id=? and bio_disease_id=? and etl_source=?"
		if(biomart.firstRow(qry, [
			bioDataId,
			bioDiseaseId,
			etlSource
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
