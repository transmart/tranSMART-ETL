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

class SnpCopyNumber {

	private static final Logger log = Logger.getLogger(SnpCopyNumber)

	Sql deapp
	File copyNumberFile

	void loadSnpCopyNumber(int batchSize){

		if(copyNumberFile.size() > 0) {
			
			log.info("Start loading Copy Number from ${copyNumberFile.toString()} into DE_SNP_COPY_NUMBER ...")

			String [] str
			String qry = " insert into DE_SNP_COPY_NUMBER(patient_num, snp_name, chrom, chrom_pos, copy_number) values(?, ?, ?, ?, ?)"

			deapp.withTransaction {
				deapp.withBatch(batchSize, qry, { stmt ->
					copyNumberFile.eachLine { 
						str = it.split("\t")
						stmt.addBatch([
							str[0],
							str[1],
							str[2],
							str[3],
							str[4]
						])
					}
				})
			}
		} else {
			log.info " ${copyNumberFile.toString()} is empty or doesn't exist ... "
		}
	}


	void insertSnpCopyNumber(long patientNum, String snpName, String chr, int pos, float copyNumber){

		String qry = "insert into DE_SNP_COPY_NUMBER(patient_num, snp_name, chrom, chrom_pos, copy_number) values(?,?,?,?,?)"

		if(!isSnpCopyNumberExist(patientNum, snpName)){
			deapp.execute(qry, [
				patientNum,
				snpName,
				chr,
				pos,
				copyNumber
			])
		}
	}


	boolean isSnpCopyNumberExist(long patientNum, String snpName){

		String qry = """select count(1) from DE_SNP_COPY_NUMBER
						where snp_name=? and patient_num=? """

		def obj = deapp.firstRow(qry, [
			snpName,
			patientNum
		])

		if(obj[0] > 0) return true
		else return false
	}


	void setCopyNumberFile (File copyNumberFile){
		this.copyNumberFile = copyNumberFile
	}


	def setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}
}
