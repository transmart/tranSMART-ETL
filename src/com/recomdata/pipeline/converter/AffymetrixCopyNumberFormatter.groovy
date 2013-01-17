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

import org.apache.log4j.Logger;

import com.recomdata.pipeline.util.Util

class AffymetrixCopyNumberFormatter {

	private static final Logger log = Logger.getLogger(AffymetrixCopyNumberFormatter)

	String copyNumberFileDirectory, copyNumberFilePrefix, studyName, outputDirectory
	String sourceCopyNumberFilePattern
	File copyNumberOutputFile
	Map  experimentPatientMap

	def createCopyNumberFile(){

		log.info "Start creating Copy Number file ..."
		log.info("Search CopyNumber file from " + copyNumberFileDirectory)

		File cn = new File(copyNumberFileDirectory)
		if(cn.isDirectory()){
			cn.eachFile{
				log.info "Checking file: " + it.toString()
				if(it.toString().indexOf(sourceCopyNumberFilePattern) > 0) {
					String experimentId = it.getName().replace(sourceCopyNumberFilePattern, "")
					if(!experimentPatientMap[experimentId].equals(null))
						processCopyNumberFile(it)
				}
			}
		}
	}


	void processCopyNumberFile(File file){

		boolean isCopyNumberData = false
		boolean isDataSection = false

		long expectedDataRows = 0 , totalDataRows = 0
		int totalDataColumns = 0
		String sampleName, outputDirectory

		StringBuffer sb = new StringBuffer()
		StringBuffer sbAll = new StringBuffer()

		log.info("Start processing " + file.toString())

		Map cnByChr = [:]
		String fileName = file.name
		file.eachLine{ line ->
			if(isDataSection) isCopyNumberData = true
			if(line.equals("[Data]")) isDataSection = true

			// check if file name is consistent with file content
			if(line.indexOf("SampleName=")==0) {
				def str = line.split("=")
				sampleName = str[1].trim()
				checkNameConsistency(fileName, sampleName)
			}

			// extract total data rows
			if(line.indexOf("NumberDataRows=")==0) {
				def str = line.split("=")
				try {
					expectedDataRows = Long.parseLong(str[1].trim())
					log.info("NumberDataRows in " + fileName + ": " + expectedDataRows)
				} catch (Exception e){
					log.warn("NumberDataRows in " + fileName + " is missing")
				}
			}

			// extract total columns
			if(line.indexOf("NumberDataColumns=")==0) {
				def str = line.split("=")
				try {
					totalDataColumns = Integer.parseInt(str[1].trim())
					log.info("NumberDataColumns in " + fileName + ": " + totalDataColumns)
				} catch (Exception e){
					log.warn("NumberDataColumns in " + fileName + " is missing")
				}
			}

			String chr
			if(isCopyNumberData){
				//if(line.indexOf("SNP_A-") != -1){
				totalDataRows++

				// 0 - SNP ID; 1 - Chr; 3 - Position; 5 - Copy Number
				def str = line.split("\t")
				chr = str[1].trim()
				sb.append(experimentPatientMap[sampleName] + "\t")
				sb.append(str[0].trim() + "\t")
				sb.append(chr + "\t")
				sb.append(str[2].trim() + "\t")
				sb.append(str[5].trim() + "\n")

				sbAll.append(sb.toString())
			}

			if(!chr.equals(null)){
				//writeCopyNumberFileByChr(sb, chr)
				if(cnByChr[chr].equals(null)){
					cnByChr[chr] = sb.toString()
				}else{
					cnByChr[chr] += sb.toString()
				}
				sb.delete(0, sb.length())
			}
		}


		writeCopyNumberFile(sbAll)
		writeCopyNumberFileByChrs(cnByChr)

		if(totalDataRows == expectedDataRows){
			log.info(fileName + ": expected rows - " + expectedDataRows + ";processed rows - " + totalDataRows)
		}else{
			log.error(fileName + ": expected rows - " + expectedDataRows + ";processed rows - " + totalDataRows)
		}
	}


	void writeCopyNumberFile(StringBuffer sb){

		copyNumberOutputFile = getCopyNumberOutputFile()

		if(!copyNumberOutputFile.exists()) {
			log.info("Create " + copyNumberOutputFile.toString())
			copyNumberOutputFile.createNewFile()
		}
		copyNumberOutputFile.append(sb.toString())
	}


	void writeCopyNumberFileByChrs(Map cnByChr){
		cnByChr.each{key, val ->
			writeCopyNumberFileByChr(val, key)
		}
	}


	void writeCopyNumberFileByChr(String sb, String chr){
		File cn = getCopyNumberOutputFileByChr(chr)
		if(!cn.exists()){
			log.info "Creat Copy Number file for: " + cn.toString()
			cn.createNewFile()
		}

		cn.append(sb)
	}


	void checkNameConsistency(String fileName, String sampleName){
		if(fileName.indexOf(sampleName) == 0) {
			log.info(fileName + " is consistent with SampleName: " + sampleName)
		}
		else {
			log.warn(fileName + " is inconsistent with SampleName: " + sampleName)
		}
	}


	String normalizeChromosomeName(String chr){
		if(chr.equals("X")) return "23"
		else if (chr.equals("Y")) return "24"
		else if(chr.equals("XY")) return "25"
		else if (chr.equals("MT")) return "26"
		else return chr
	}


	File getCopyNumberOutputFileByChr(String chr){
		String chrName = normalizeChromosomeName(chr)
		return new File(outputDirectory + File.separator + "chr" + chrName + ".cn")
	}


	File getCopyNumberOutputFile(){
		return new File(outputDirectory + File.separator + studyName + ".cn")
	}


	void setStudyName(String studyName){
		this.studyName = studyName
	}

	void setOutputDirectory(String outputDirectory){
		this.outputDirectory = outputDirectory
	}


	void setCopyNumberOutputFile(File copyNumberOutputFile){
		this.copyNumberOutputFile = copyNumberOutputFile
	}


	void setCopyNumberFileDirectory(String copyNumberFileDirectory){
		this.copyNumberFileDirectory = copyNumberFileDirectory
	}


	void setSourceCopyNumberFilePattern(String sourceCopyNumberFilePattern){
		this.sourceCopyNumberFilePattern = sourceCopyNumberFilePattern
	}
	
	
	void setExperimentPatientMap(Map experimentPatientMap){
		this.experimentPatientMap = experimentPatientMap
	}
}
