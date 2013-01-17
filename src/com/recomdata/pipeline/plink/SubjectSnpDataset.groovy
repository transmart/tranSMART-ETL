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
  

package com.recomdata.pipeline.plink

import org.apache.log4j.Logger;

import com.recomdata.pipeline.i2b2.PatientDimension;
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql

class SubjectSnpDataset {

	private static final Logger log = Logger.getLogger(SubjectSnpDataset)

	Sql i2b2demodata, deapp
	String trialName, platform, sourceSystemPrefix
	Map concept, sampleType, patientConceptCodeMap

	PatientDimension pd

	void loadSubjectSnpDataset(Map subjectSnpDataset){

		long patientNum
		String conceptCode, subjectId, sampleType, gender
		Map subjectSnpDatasetRecord = [:]

		subjectSnpDataset.each{k, v ->
			patientNum = Integer.parseInt(k.split(":")[0])
			conceptCode = k.split(":")[1]
			subjectId = v.split (":")[0]
			sampleType  = v.split (":")[1].replace("null", "")
			gender  = v.split (":")[2]

			if(!isSubjectSnpDatasetExist(conceptCode, patientNum, trialName, platform)){

				log.info ("insert the record ($patientNum,  $conceptCode, $subjectId, $gender) into DE_SUBJECT_SNP_DATASET ...")

				subjectSnpDatasetRecord["concept_cd"] = conceptCode
				subjectSnpDatasetRecord["platform_name"] = platform
				subjectSnpDatasetRecord["patient_num"] = patientNum
				subjectSnpDatasetRecord["trial_name"] = trialName
				subjectSnpDatasetRecord["subject_id"] = subjectId
				subjectSnpDatasetRecord["patient_gender"] = gender
				subjectSnpDatasetRecord["sample_type"] = sampleType
				subjectSnpDatasetRecord["dataset_name"] = trialName + "_" + subjectId + "_" + conceptCode

				insertSubjectSnpDataset(subjectSnpDatasetRecord)
			} else{
				log.info ("The record ($patientNum,  $conceptCode, $subjectId, $gender) is already inserted into DE_SUBJECT_SNP_DATASET ...")
			}
		}
	}


	void insertSubjectSnpDataset(Map subjectSnpDatasetRecord){

		String qry = """ insert into de_subject_snp_dataset(dataset_name, concept_cd, platform_name, 
								trial_name, patient_num, subject_id, sample_type, patient_gender)
						 values(?, ?, ?, ?, ?,  ?, ?, ?) """
		deapp.execute(qry, [
			subjectSnpDatasetRecord["dataset_name"],
			subjectSnpDatasetRecord["concept_cd"],
			subjectSnpDatasetRecord["platform_name"],
			subjectSnpDatasetRecord["trial_name"],
			subjectSnpDatasetRecord["patient_num"],
			subjectSnpDatasetRecord["subject_id"],
			subjectSnpDatasetRecord["sample_type"],
			subjectSnpDatasetRecord["patient_gender"]
		])
	}


	/**
	 *   extract data from PLINK's FAM data file and insert them into 
	 *    DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC tables
	 *     
	 * @param deapp			database handler, point to DEAPP schema
	 * @param fam			name of FAM file
	 * @param platform		platform used in this SNP array
	 * @param trial			study name
	 * @return
	 */

	def loadSnpDatasetFromFam(File fam){

		String datasetName, sample, sex, subjectId, indId, conceptCode

		log.info "Start loading data into DE_SUBJECT_SNP_DATASET ..."

		String qry = """ insert into de_subject_snp_dataset(dataset_name, concept_cd, platform_name,
						      trial_name, patient_num, subject_id, sample_type, patient_gender)
						 values(?, ?, ?, ?, ?, ?, ?, ?) """

		fam.eachLine {
			String [] str = (it.indexOf("\t") != -1) ? it.split("\t") : it.split(" +")

			indId = Util.cleanupId(str[1].trim())

			// translate gender from "1" to "M" (Male),
			//        "2" to "F" (Female), and other to "U" (Unknown)
			if(str[4].equals("1")) sex = 'M'
			else if(str[4].equals("2")) sex = 'F'
			else sex = 'U'

			int patientNum = 0
			if(pd.isPatientNumber(indId)){
				patientNum = Integer.parseInt(indId)
			}else{
				String individualId = sourceSystemPrefix + ":" + indId
				long pid = pd.getPatientNumberByIndividualId(individualId)
				if(pid > 0){
					patientNum = pid
				}else{
					log.error("Cannot find patient_number for: " + individualId)
					throw new RuntimeException("Cannot find patient_number for: " + individualId)
				}
			}

			subjectId =   patientConceptCodeMap[patientNum].split(":")[0]
			sample =  patientConceptCodeMap[patientNum].split(":")[1].toString().replace("null", "")
			conceptCode = patientConceptCodeMap[patientNum].split(":")[2]
			datasetName = (sample.equals(null) || sample.size()==0) ? (trial + "_" + subjectId) : (trial + "_" + subjectId + "_" + sample)

			if(!isSubjectSnpDatasetExist(datasetName, patientNum))
				deapp.execute(qry, [
					datasetName,
					conceptCode,
					platform,
					trial,
					patientNum,
					subjectId,
					sample,
					sex
				])
		}

		log.info "End loading data into DE_SUBJECT_SNP_DATASET ..."
	}


	boolean isSubjectSnpDatasetExist(String conceptCode, long patientNum, String trialName, String platform){

		String qry = """ select count(1) from de_subject_snp_dataset
						  where concept_cd=? and patient_num=? and trial_name=? and platform_name=?"""

		if(deapp.firstRow (qry, [
			conceptCode,
			patientNum,
			trialName,
			platform
		])[0]==0)
			return false
		else
			return true
	}



	boolean isSubjectSnpDatasetExist(String datasetName, long patientNum){

		String qry = """ select count(1) from de_subject_snp_dataset
						  where dataset_name=? and patient_num=?"""

		if(deapp.firstRow (qry, [datasetName, patientNum])[0]==0)
			return false
		else
			return true
	}


	/**
	 *   extract data from DE_SUBJECT_SNP_DATASET table and populate into
	 *     DE_SNP_DATA_DATASET_LOC table
	 *     
	 * @param deapp		database handler
	 * @return
	 */

	def loadSnpDatasetLocation(){

		log.info "Start loading data into DE_SNP_DATA_DATASET_LOC ..."

		String qry = "select count(1) from user_sequences where sequence_name=?"

		def obj = deapp.firstRow(qry, ['LOC'])
		if(obj[0] == 0) {
			qry = "create sequence loc"
			deapp.execute(qry)
		}else{
			qry = "drop sequence loc"
			deapp.execute(qry)

			qry = "create sequence loc"
			deapp.execute(qry)
		}

		qry = """insert into de_snp_data_dataset_loc(snp_dataset_id, trial_name, location)
		         select subject_snp_dataset_id, trial_name, loc.nextval
		         from de_subject_snp_dataset
		         where subject_snp_dataset_id not in (select snp_dataset_id from de_snp_data_dataset_loc) """
		deapp.execute(qry)

		qry = "drop sequence loc"
		deapp.execute(qry)

		log.info "End loading data into DE_SNP_DATA_DATASET_LOC ..."
	}


	/**
	 *   set database handler to retrieve patient_num from i2b2demodata.patient_dimension
	 *   
	 * @param i2b2demodata		database handler, point to I2B2DEMODATA schema
	 * @return
	 */

	def setSqlForI2b2demodata(Sql i2b2demodata){
		this.i2b2demodata = i2b2demodata
	}

	/**
	 * 
	 * @return
	 */
	def getSqlForI2b2demodata(){
		return i2b2demodata
	}

	/**
	 * 
	 * @param deapp
	 * @return
	 */
	def setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}

	/**
	 * 
	 * @param trial
	 * @return
	 */
	def setTrialName(String trialName){
		this.trialName = trialName
	}

	/**
	 * 
	 * @param platform
	 * @return
	 */
	def setPlatform(String platform){
		this.platform = platform
	}

	/**
	 * 
	 * @param concept
	 * @return
	 */
	def setConcept(Map concept){
		this.concept = concept
	}

	/**
	 * 
	 * @param sampleType
	 * @return
	 */
	def setSampleType(Map sampleType){
		this.sampleType = sampleType
	}

	
	Map getPatientSnpDatasetMap(){

		Map patientDatasetMap = [:]
		
		String qry = """ select subject_snp_dataset_id, patient_num, subject_id from de_subject_snp_dataset
						 where trial_name=? """

		deapp.eachRow(qry, [trialName]) {
			patientDatasetMap[it.patient_num] = it.subject_snp_dataset_id + ":" + it.subject_id
		}
		
		return patientDatasetMap
	}

	
	Map getSnpDatasetId(){

		Map patientDatasetMap = [:]
		
		String qry = """ select subject_snp_dataset_id, patient_num from de_subject_snp_dataset
						 where trial_name=? """

		deapp.eachRow(qry, [trialName]) {
			patientDatasetMap[it.patient_num] = it.subject_snp_dataset_id
		}
		
		return patientDatasetMap
	}
	

	Map getSnpDatasetId(String trialName){

		Map patientDatasetMap = [:]
		
		String qry = """ select subject_snp_dataset_id, patient_num from de_subject_snp_dataset
						 where trial_name=? """

		deapp.eachRow(qry, [trialName]) {
			patientDatasetMap[it.patient_num] = it.subject_snp_dataset_id
		}
		
		return patientDatasetMap
	}



	def getSnpDatasetId(String trial,  String subjectId){

		String qry = """ select subject_snp_dataset_id from de_subject_snp_dataset
								 where trial_name=?  and subject_id=? """

		def datasetIds = deapp.firstRow(qry, [trial, subjectId])


		if(datasetIds.size() > 1) {
			log.error "There are multiple dataset_ids ..."
			return null
		} else {
			return datasetIds[0]
		}
	}


	/**
	 *  retrieve dataset_id for a particular study's patient's sample
	 *   
	 * @param trial			study name
	 * @param sample		sample type
	 * @param subjectId		subject id used in this study
	 * @return
	 */
	def getSnpDatasetId(String trial, String sample, String subjectId){

		String qry = """ select subject_snp_dataset_id from de_subject_snp_dataset 
		                 where trial_name=? and sample_type=?  and subject_id=? """

		def datasetIds = deapp.firstRow(qry, [trial, sample, subjectId])


		if(datasetIds.size() > 1) {
			log.error "There are multiple dataset_ids ..."
			return null
		} else {
			return datasetIds[0]
		}
	}


	/**
	 *  retrieve dataset_id for a particular study's patient's sample
	 *
	 * @param trial			study name
	 * @param subjectId		subject id used in this study
	 * @return
	 */
	def getSnpDatasetId(String trial, String subjectId, long patientNum){

		String qry = """ select subject_snp_dataset_id from de_subject_snp_dataset
						where trial_name=?  and (patient_num=? or subject_id=?) """

		def datasetIds = deapp.firstRow(qry, [trial, patientNum, subjectId])

		if(datasetIds.equals(null) || datasetIds.size() > 1) {
			log.error "There is either no or multiple dataset_ids for " + subjectId
			throw new RuntimeException("There is either no or multiple dataset_ids for " + subjectId)
		} else {
			return datasetIds[0]
		}
	}


	def getSnpDatasetIdByPatientNumber(String trial, int patientNum){

		String qry = """ select subject_snp_dataset_id from de_subject_snp_dataset
								where trial_name=?  and patient_num=? """

		def datasetIds = deapp.firstRow(qry, [trial, patientNum])


		if(datasetIds.size() > 1) {
			log.error "There are multiple dataset_ids ..."
			return null
		} else {
			return datasetIds[0]
		}
	}


	def setSourceSystemPrefix(String sourceSystemPrefix){
		this.sourceSystemPrefix = sourceSystemPrefix
	}


	void setPatientDimension(PatientDimension pd){
		this.pd = pd
	}


	void setPatientConceptCodeMap(Map patientConceptCodeMap){
		this.patientConceptCodeMap = patientConceptCodeMap
	}
}
