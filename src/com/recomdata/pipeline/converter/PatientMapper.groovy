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
  

package com.recomdata.pipeline.converter


import com.recomdata.pipeline.i2b2.PatientDimension
import groovy.sql.Sql
import org.apache.log4j.Logger;

class PatientMapper {

	private static final Logger log = Logger.getLogger(PatientMapper)

	Sql sql
	String studyName
	Map gsmMap = [:], gsmToGsmMapping
	PatientDimension patientDimension 

	Map getPatientMapFromSampleMap(Map sampleMapping){
		Map patientMap = [:]
		long patientNumber

		sampleMapping.each{key, val ->

			if(key.indexOf("experiment_id") == -1){

				String [] str = val.split(":")
				long patientNum1 = getPatientNumberBySubjectId(str[0])
				long patientNum2 = getPatientNumberBySubjectId(str[1])
				if(patientNum1 == 0 && patientNum2 == 0){

					addPatient(val)

					patientNumber = getPatientNumberBySubjectId(val)
					patientMap[key] = patientNumber
					gsmMap[str[0]] = patientNumber
					gsmMap[str[1]] = patientNumber

					log.info("Add patient record for sample: " + val)
				}else{
					if(patientNum1 != patientNum2) {
						String errMsg = "Inconsistent patient number for " + str[0] + "(" + patientNum1 + ") and " + str[1] + "(" + patientNum2 + ")"
						log.error(errMsg)
						throw new RuntimeException(errMsg)
					}else{
						patientMap[key] = patientNum1
						gsmMap[str[0]] = patientNum1
						gsmMap[str[1]] = patientNum1
						log.info(key + " --> " + val + " --> " + patientNum1)
					}
				}
			}
		}

		return patientMap
	}

	
	Map getPatientMapFromGsmMap(Map gsmToGsmMapping){
		Map patientGsmMap = [:]
		long patientNumber

		File update = new File("C:/SNP/GSE14860/update.sql")
		
		gsmToGsmMapping.each{key, val ->

			if(val.indexOf("SubjectId") == -1){

				String [] str = val.split(":")
				long patientNum1 = getPatientNumberBySubjectId(str[0])
				long patientNum2 = getPatientNumberBySubjectId(str[1])
				long patientNum3 = getPatientNumberBySubjectId(str[2])
				if(patientNum1 == 0 && patientNum2 == 0){

					//addPatient(val)

					patientNumber = getPatientNumberBySubjectId(val)
					patientGsmMap[key] = patientNumber
					gsmMap[str[0]] = patientNumber
					gsmMap[str[1]] = patientNumber

					log.info("Add patient record for sample: " + val)
				}else{
					if(patientNum1 != patientNum2) {
						String errMsg = "Inconsistent patient number for " + str[0] + "(" + patientNum1 + ") and " + str[1] + "(" + patientNum2 + ")"
						log.error(errMsg)
						throw new RuntimeException(errMsg)
					}else{
						patientGsmMap[key] = patientNum1
						gsmMap[str[0]] = patientNum1
						gsmMap[str[1]] = patientNum1
						log.info(key + " --> " + val + " --> " + patientNum1 + ":" + patientNum3)
						StringBuffer line = new StringBuffer()
						line.append("update pt set sample_id='" + key + "' where patient_num=" + patientNum1 + ";\n")
						line.append("update pt set sample_id='" + key + "' where patient_num=" + patientNum3 + ";\n")
						
						//line.append "update DE_SNP_DATA_BY_PATIENT set patient_num=" + patientNum3 + " where patient_num = " + patientNum1 + ";\n" 
						//line.append "update DE_SNP_SUBJECT_SORTED_DEF set patient_num=" + patientNum3 + " where patient_num = " + patientNum1 + ";\n"
						//line.append "update DE_SUBJECT_SNP_DATASET set patient_num=" + patientNum3 + " where patient_num = " + patientNum1 + ";\n"
						update.append(line.toString())
					}
				}
			}
		}

		return patientGsmMap
	}

	
	long getPatientNumberByIndividual(String individualId){
		String qry = "select patient_num from patient_dimension where sourcesystem_cd ='" + individualId + "'"
		def v = sql.firstRow(qry)

		if(v.equals(null)) {
			log.warn("No patient number found for " + individualId)
			return 0
		}else{
			return v[0]
		}
	}

	long getPatientNumberBySubjectId(String subjectId){
		String qry = "select patient_num from patient_dimension where sourcesystem_cd like '%" + subjectId + "%'"
		def v = sql.firstRow(qry)

		if(v.equals(null)) {
			log.warn("No patient number found for " + subjectId)
			return 0
		}else{
			return v[0]
		}
	}

	void addPatient(String subjectId){
		String qry = "insert into patient_dimension(sourcesystem_cd) values(?)"
		sql.execute(qry, [studyName + ":" + subjectId])
	}

	
	void setPatientDimension(PatientDimension patientDimension){
		this.patientDimension = patientDimension
	}

	void setStudyName(String studyName){
		this.studyName = studyName
	}

	void setSql(Sql sql){
		this.sql = sql
	}

	Map getGsmMap(){
		return this.gsmMap
	}
}
