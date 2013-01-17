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

import com.recomdata.pipeline.util.Util

import groovy.sql.Sql

class SnpCallsByGsm {

	private static final Logger log = Logger.getLogger(SnpCallsByGsm)

	Sql deapp
	
	// patient_num -> sample id or GSM# mapping
	Map patientSampleMap

	// in the format: GSM#   snp_name   snp_calls
	File genotypeFile

	void loadSnpCallsByGsm(int batchSize){

		if(genotypeFile.size() > 0) {

			log.info("Start loading Copy Number from ${genotypeFile.toString()} into DE_SNP_CALLS_BY_GSM ...")

			String [] str
			String qry = " insert into DE_SNP_CALLS_BY_GSM (gsm_num, patient_num, snp_name, snp_calls) values(?, ?, ?, ?)"

			deapp.withTransaction {
				deapp.withBatch(batchSize, qry, { stmt ->
					genotypeFile.eachLine {
						str = it.split("\t")
						stmt.addBatch([
							str[0],
							patientSampleMap[str[0]],
							str[1],
							str[2]
						])
					}
				})
			}
		} else {
			log.info " ${genotypeFile.toString()} is empty or doesn't exist ... "
		}
	}


	void insertSnpCallsByGsm(String gsmNum, long patientNum, String snpName, String genotype){

		String qry = "insert into DE_SNP_COPY_NUMBER(gsm_num, patient_num, snp_name, snp_calls) values(?, ?, ?, ?)"

		if(!isSnpCallsByGsmExist(gsmNum, snpName)){
			deapp.execute(qry, [
				gsmNum,
				patientNum,
				snpName,
				genotype
			])
		}
	}


	boolean isSnpCallsByGsmExist(String gsmNum, String snpName){

		String qry = """select count(1) from DE_SNP_CALLS_BY_GSM
						where snp_name=? and gsm_num=? """

		def obj = deapp.firstRow(qry, [snpName, gsmNum])

		if(obj[0] > 0) return true
		else return false
	}


	void setGenotypeFile (File genotypeFile){
		this.genotypeFile = genotypeFile
	}


	void setPatientSampleMap(Map patientSampleMap){
		this.patientSampleMap = patientSampleMap
	}


	def setSql(Sql deapp){
		this.deapp = deapp
	}
}
