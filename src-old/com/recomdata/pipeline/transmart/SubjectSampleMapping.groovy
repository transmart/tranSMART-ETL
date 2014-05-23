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

import com.recomdata.pipeline.util.Util

class SubjectSampleMapping {

	private static final Logger log = Logger.getLogger(SubjectSampleMapping)

	Sql deapp
	List subjectSamples
	Map subjectPatientMap, conceptPathToCode
	String platformPath, platform

	void loadSubjectSampleMapping(){

		log.info platformPath
		Util.printMap(subjectPatientMap)
		Util.printMap(conceptPathToCode)

		Map dataMap = [:]
		subjectSamples.each{
			dataMap = it

			dataMap["PATIENT_ID"] = subjectPatientMap[dataMap["SUBJECT_ID"]]
			dataMap["SAMPLE_ID"] = dataMap["PATIENT_ID"]
			dataMap["SOURCE_CD"] = "STD"
			dataMap["PLATFORM_CD"] = conceptPathToCode[platformPath]

			if(dataMap["SAMPLE_TYPE"].equals(null) || dataMap["SAMPLE_TYPE"].toString().size()==0)
				dataMap["SAMPLE_TYPE_CD"] =  conceptPathToCode[platformPath]
			else
				dataMap["SAMPLE_TYPE_CD"] =  conceptPathToCode[platformPath + dataMap["SAMPLE_TYPE"] + "/"]

			dataMap["CONCEPT_CODE"] = dataMap["SAMPLE_TYPE_CD"]
			dataMap["DATA_UID"] = dataMap["CONCEPT_CODE"] + "-" + dataMap["PATIENT_ID"]

			//String [] str = platformPath.split("/")
			dataMap["PLATFORM"] = platform //str[-2]
			//if(dataMap["PLATFORM"].toString().toUpperCase().indexOf("SNP") != -1) dataMap["PLATFORM"] = "SNP"

			insertSubjectSampleMapping(dataMap)
			dataMap = [:]
		}
	}

	void insertSubjectSampleMapping(Map dataMap){
		String qry = """ insert into de_subject_sample_mapping (
							PATIENT_ID,
							SUBJECT_ID,
							CONCEPT_CODE,
							ASSAY_ID,
							SAMPLE_TYPE,
							TRIAL_NAME,
							SAMPLE_TYPE_CD,
							PLATFORM,
							PLATFORM_CD,
							DATA_UID,
							GPL_ID,
							SAMPLE_ID,
							SAMPLE_CD,
							CATEGORY_CD,
							SOURCE_CD)
						values(?,?,?,seq_assay_id.nextval,?, ?,?,?,?,?, ?,?,?,?,?)"""

		if(isSubjectSampleMappingExist(dataMap)){
			log.info "Exist a record for $dataMap"
		} else {
			log.info "Insert a record for $dataMap"
			deapp.execute(qry, [
				dataMap["PATIENT_ID"],
				dataMap["SUBJECT_ID"],
				dataMap["CONCEPT_CODE"],
				//dataMap["ASSAY_ID"],
				dataMap["SAMPLE_TYPE"],
				dataMap["TRIAL_NAME"],
				dataMap["SAMPLE_TYPE_CD"],
				dataMap["PLATFORM"],
				dataMap["PLATFORM_CD"],
				dataMap["DATA_UID"],
				dataMap["GPL_ID"],
				dataMap["SAMPLE_ID"],
				dataMap["SAMPLE_CD"],
				dataMap["CATEGORY_CD"],
				dataMap["SOURCE_CD"]
			])
		}
	}



	boolean isSubjectSampleMappingExist(Map dataMap){

		String qry = """ select count(*) from de_subject_sample_mapping 
		                 where patient_id=? and trial_name=? and sample_cd=? and platform=? """
		def res = deapp.firstRow(qry, [
			dataMap["PATIENT_ID"],
			dataMap["TRIAL_NAME"],
			dataMap["SAMPLE_CD"],
			dataMap["PLATFORM"]
		])
		if(res[0] > 0) return true
		else return false
	}



	Map getSamplePatientMap(String studyName, String platform){

		Map samplePatientMap = [:]

		String qry = """select sample_cd, patient_id from de_subject_sample_mapping 
                        where platform=? and trial_name=? """
		deapp.eachRow(qry, [platform, studyName]) {
			samplePatientMap[it.sample_cd] = it.patient_id
		}

		return samplePatientMap
	}



	Map getSamplePatientMap(String studyName){

		Map samplePatientMap = [:]

		String qry = """select sample_cd, patient_id from de_subject_sample_mapping trial_name=? """
		deapp.eachRow(qry, [studyName]) {
			samplePatientMap[it.sample_cd] = it.patient_id
		}

		return samplePatientMap
	}


	Map getSubjectSampleMapping(){

		Map subjectSampleMapping = [:]

		String qry = "select sample_type, concept_code from de_subject_snp_dataset "
		deapp.eachRow(qry) {
			subjectSampleMapping[it.subject_id] = it.patient_num
		}

		return subjectSampleMapping
	}


	Map getSampleConceptCodeMap(String trialName){

		Map sampleConceptCodeMap = [:]

		String qry = "select sample_type, concept_code from de_subject_sample_mapping where trial_name = ?"
		deapp.eachRow(qry, [trialName]) {
			sampleConceptCodeMap[it.sample_type] = it.concept_code
		}

		return sampleConceptCodeMap
	}



	/**
	 * 
	 * @param trialName
	 * @param platform	   "SNP" for SNP data
	 * @return a map with the format:   patient_num:concept_code -> subject_id:sample_type
	 */
	Map getPatientConceptCodeMap(String trialName, String platform){

		log.info "Extract a map [patient_id -> subject_id:sample_type:concept_code] from DE_SUBJECT_SAMPLE_MAPPING ... "

		Map map = [:]
		String qry = """ select patient_id, subject_id, sample_type, concept_code
						 from de_subject_sample_mapping 
                         where trial_name = ? and platform=? """
		deapp.eachRow(qry, [trialName, platform]) {
			map[it.patient_id + ":" + it.concept_code] = it.subject_id + ":" + it.sample_type
		}

		return map
	}

	
	
	/**
	 *
	 * @param trialName
	 * @param platform	   "SNP" for SNP data
	 * @return a map with the format: sample_cd (GSM#) -> patient_num
	 */
	Map getPatientSampleMap(String trialName, String platform){

		log.info "Extract a map [sample_cd (GSM#) -> patient_id] from DE_SUBJECT_SAMPLE_MAPPING ... "

		Map map = [:]
		String qry = """ select patient_id, sample_cd
						 from de_subject_sample_mapping
						 where trial_name = ? and platform=? """
		deapp.eachRow(qry, [trialName, platform]) {
			map[it.sample_cd] = it.patient_id
		}

		return map
	}

	
	Map getPatientConceptCodeMap(String trialName){

		log.info "Extract a map [patient_id -> subject_id:sample_type:concept_code] from DE_SUBJECT_SAMPLE_MAPPING ... "

		Map map = [:]
		String qry = """ select patient_id, subject_id, sample_type, concept_code 
                         from de_subject_sample_mapping where trial_name = ? """
		deapp.eachRow(qry, [trialName]) {
			map[(int) it.patient_id] = it.subject_id + ":" + it.sample_type + ":" + it.concept_code
		}

		return map
	}


	void setPlatformPath(String platformPath){
		this.platformPath = platformPath
	}


	void setconceptPathToCode(Map conceptPathToCode){
		this.conceptPathToCode = conceptPathToCode
	}


	void setSubjectPatientMap(Map subjectPatientMap){
		this.subjectPatientMap = subjectPatientMap
	}


	void setPlatform(String platform){
		this.platform = platform
	}


	void setSubjectSamples(List subjectSamples){
		this.subjectSamples = subjectSamples
	}

	void setSql(Sql deapp){
		this.deapp = deapp
	}
}
