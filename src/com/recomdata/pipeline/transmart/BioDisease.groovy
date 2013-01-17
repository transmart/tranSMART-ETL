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

class BioDisease {

	private static final Logger log = Logger.getLogger(BioDisease)

	Sql biomart

	void loadMeshBioDisease(String disease, String meshCode){

		if(isMeshBioDiseaseExist(disease, meshCode)){
			log.info "($disease, $meshCode) already exists in BIO_DISEASE ..."
		}else{
			log.info "Start loading ($disease, $meshCode) into BIO_DISEASE ..."

			String qry = """ insert into bio_disease(disease, mesh_code) values(?, ?) """
			biomart.execute(qry, [
				disease,
				meshCode
			])

			log.info "End loading ($disease, $meshCode) into BIO_DISEASE ..."
		}
	}


	boolean isMeshBioDiseaseExist(String disease, String meshCode){
		String qry = "select count(1) from bio_disease where disease=? and mesh_code=?"
		if(biomart.firstRow(qry, [
			disease,
			meshCode
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
