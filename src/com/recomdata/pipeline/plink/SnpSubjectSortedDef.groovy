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

import java.io.File;

import org.apache.log4j.Logger;

import com.recomdata.pipeline.i2b2.PatientDimension
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql

class SnpSubjectSortedDef {

	private static final Logger log = Logger.getLogger(SnpSubjectSortedDef)

	String trialName
	Sql deapp
	Map patientSubjectMap
	File pedFile

	void loadSubjectSorteDef(){

		if(isSubjectSortedDefExist(trialName)){
			log.info " There are records for $trialName in DE_SNP_SUBJECT_SORTED_DEF ... "
		} else {
			log.info("Start loading records for $trialName into DE_SNP_SUBJECT_SORTED_DEF ...")

			Map orderedPatientNumberList = getOrderedPatientNumberList()

			String qry = " insert into DE_SNP_SUBJECT_SORTED_DEF(trial_name, patient_position, patient_num, subject_id) values(?,?,?,?)"

			deapp.withTransaction {
				deapp.withBatch(qry, { stmt ->
					orderedPatientNumberList.each { k, v ->
						stmt.addBatch([
							trialName,
							k,
							v,
							patientSubjectMap[v]
						])
					}
				})
			}
			
			log.info("End loading records for $trialName into DE_SNP_SUBJECT_SORTED_DEF ...")
		}
	}


	void insertSubjectSorteDef(long patientNum, int position, String trialName, String subjectId){

		String qry = "insert into DE_SNP_SUBJECT_SORTED_DEF(trial_name, patient_position, patient_num, subject_id) values(?,?,?,?)"

		if(!isSubjectSortedDefExist(trialName, patientNum, subjectId)){
			deapp.execute(qry, [
				trialName,
				position,
				patientNum,
				subjectId
			])
		}
	}


	/**
	 *  Check if there are any records for this study/trial
	 *   
	 * @param trialName		the name of trial or study
	 * @return
	 */
	boolean isSubjectSortedDefExist(String trialName){
		String qry = "select count(1) from DE_SNP_SUBJECT_SORTED_DEF where trial_name=? "
		def obj = deapp.firstRow(qry, [trialName])
		if(obj[0] > 0) return true
		else return false
	}



	boolean isSubjectSortedDefExist(String trialName, long patientNum, String subjectId){

		String qry = """select count(1) from DE_SNP_SUBJECT_SORTED_DEF 
		                where trial_name=? and patient_num=? and subject_id=? """

		def obj = deapp.firstRow(qry, [
			trialName,
			patientNum,
			subjectId
		])

		if(obj[0] > 0) return true
		else return false
	}


	Map getOrderedPatientNumberList(){

		Map patientNumberOrderedList = [:]

		if(pedFile.exists() && pedFile.size() > 0) {
			log.info "Extract subject sorted definition from PED file: " + pedFile.toString()
			int index = 0
			String [] str
			pedFile.eachLine {
				index++
				str = it.split(" +")
				patientNumberOrderedList[index] = str[0]
			}
		}
		else{
			log.error "PED file: " + pedFile.toString() + " doesn't exist or is empty ... "
		}

		return patientNumberOrderedList
	}


	void setPedFile(File pedFile){
		this.pedFile = pedFile
	}


	void setPatientSubjectMap(Map patientSubjectMap){
		this.patientSubjectMap = patientSubjectMap
	}


	void setTrialName(String trialName){
		this.trialName = trialName
	}


	def setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}
}
