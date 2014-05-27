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
  

package com.recomdata.pipeline.i2b2

import org.apache.log4j.Logger;

import groovy.sql.Sql

class ObservationFact {

	private static final Logger log = Logger.getLogger(ObservationFact)

	Sql i2b2demodata
	Map conceptPathToCode, subjectToPatient
	String studyName, basePath


	void loadObservationFact(Map subjects){

		log.info "Start loading OBSERVATION_FACT ..."

		String conceptPath
		subjects.each{key, val ->

			long patientNum =subjectToPatient[key]
			if(val.equals(null) || val.size() ==0 )   conceptPath = basePath
			else conceptPath = basePath + val + "/"
			String conceptCode = conceptPathToCode[conceptPath]

			insertObservationFact(patientNum, conceptCode)
		}
	}


	void insertObservationFact(long patientNum, String conceptCode){


		String qry = """ insert into observation_fact (patient_num, concept_cd, modifier_cd
							,valtype_cd
							,tval_char
							,nval_num
							,sourcesystem_cd
							,import_date
							,valueflag_cd
							,provider_id
							,location_cd
							)
						 values(?, ?, ?
								  ,'T' -- Text data type
								  ,'E'  --Stands for Equals for Text Types
								  ,null	--	not numeric for Proteomics
								  ,?
								  ,sysdate
								  ,'@'
								  ,'@'
								  ,'@')
							""";

		if(isObservationFactExist(patientNum, conceptCode)){
			log.info "($patientNum, $conceptCode) already exists in OBSERVATION_FACT ..."
		}else{
			i2b2demodata.execute(qry, [
				patientNum,
				conceptCode,
				studyName,
				studyName
			])
		}
	}



	boolean isObservationFactExist(long patientNum, String conceptCode){
		String qry = "select count(*) from observation_fact where patient_num=? and concept_cd=?"
		def res = i2b2demodata.firstRow(qry, [patientNum, conceptCode])
		if(res[0] > 0) return true
		else return false
	}


	void setBasePath(String basePath){
		this.basePath = basePath
	}


	void setStudyName(String studyName){
		this.studyName = studyName
	}

	void setSubjectToPatient(Map subjectToPatient){
		this.subjectToPatient = subjectToPatient
	}


	void setConceptPathToCode(Map conceptPathToCode){
		this.conceptPathToCode = conceptPathToCode
	}


	void setI2b2demodata(Sql i2b2demodata){
		this.i2b2demodata = i2b2demodata
	}
}
