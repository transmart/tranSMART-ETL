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

class BioObservation {

	private static final Logger log = Logger.getLogger(BioObservation)

	Sql biomart

	void loadBioObservation(Map obs){

		String obsName, obsCode, obsDescr, obsType, obsCodeSource, qry
		obs.each{ k, v ->
			obsName = k
			obsCode = v.split("\t")[0]
			obsDescr = v.split("\t")[1]
			obsType = v.split("\t")[2]
			obsCodeSource = v.split("\t")[3]


			//if(isBioObservationExist(obsName, obsCode)){
			if(isBioObservationExist(obsCode)){
				log.info "($obsName, $obsCode) already exists in BIO_OBSERVATION ..."
			}else{
				log.info "Load ($obsName, $obsCode) into BIO_OBSERVATION ..."

				qry = """ insert into bio_observation(obs_name, obs_code, obs_descr, obs_type, obs_code_source) values(?, ?, ?, ?, ?) """
				biomart.execute(qry, [
					obsName,
					obsCode,
					obsDescr,
					obsType,
					obsCodeSource
				])
			}
		}
	}


	int getBioObservationId(String obsCode, String obsType, String obsCodeSource){

		String qry = """ select bio_observation_id from bio_observation
						 where obs_code=? and obs_type=? and obs_code_source=?
					 """

		return biomart.firstRow(qry, [
			obsCode,
			obsType,
			obsCodeSource
		])
	}



	Map getBioObservationId(String obsType, String obsCodeSource){

		Map obsCodeMap = [:]

		String qry = """ select bio_observation_id, obs_code from bio_observation
						 where obs_type=? and obs_code_source=?
					 """
		biomart.eachRow(qry, [obsType, obsCodeSource]) {
			obsCodeMap[it.obs_code] = it.bio_observation_id
		}

		return obsCodeMap
	}



	Map getBioObservationId(){

		Map obsCodeMap = [:]

		String qry = """ select bio_observation_id, obs_code from bio_observation """
		biomart.eachRow(qry) {
			obsCodeMap[it.obs_code] = it.bio_observation_id
		}

		return obsCodeMap
	}


	boolean isBioObservationExist(String obsCode){
		String qry = "select count(1) from bio_observation where obs_code=?"
		if(biomart.firstRow(qry, [obsCode])[0] > 0){
			return true
		}else{
			return false
		}
	}


	boolean isBioObservationExist(String obsName, String obsCode){
		String qry = "select count(1) from bio_observation where obs_name=? and obs_code=?"
		if(biomart.firstRow(qry, [
			obsName,
			obsCode
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
