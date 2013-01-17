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

class AffymetrixGenotypingDataFormatter {

	private static final Logger log = Logger.getLogger(AffymetrixGenotypingDataFormatter)

	String genotypingFileDirectory, outputDirectory, studyName
	String sourceGenotypingFilePattern
	Map celPatientMap, celSampleCdMap


	void createGenotypingFile(){
		File gt = new File(genotypingFileDirectory)

		gt.eachFileRecurse {
			if(it.toString().indexOf(sourceGenotypingFilePattern) >= 0 ) {
				log.info("Processing " + it)
				processGenotypingFile(it)
			}
		}
	}

	
	void processGenotypingFile(File gtInputFile){
		
		long probeCount = 0
		int expectedCelCount
		int celCount
		def samples = []

		boolean isHeaderLine = false
		boolean isGenotypingdata = false

		File genotypingDataOutputFile = getGenotypingDataOutputFile()
		File gtOutputFileWithGsm = getGenotypingDataOutputFileWithGsm()

		String [] str
		StringBuffer sb = new StringBuffer()
		StringBuffer sbGsm = new StringBuffer()

		println "Print celPatientMap"
		Util.printMap(celPatientMap)
		println "Print celSampleCdMap"
		Util.printMap(celSampleCdMap)
		
		gtInputFile.eachLine {

			if(it.indexOf("#%affymetrix-algorithm-param-apt-opt-cel-count") >= 0 ) {
				str = it.split("=")
				expectedCelCount = Integer.parseInt(str[1].trim())
				log.info "Expected CEL Count: " + expectedCelCount
			}

			if(it.indexOf("probeset_id") == 0){
				isHeaderLine = true
				str = it.split("\t")
				for(int i in 1..str.size()-1){
					samples[i] = str[i].trim() //.replace(".CEL", "")
				}
			}
			
			if(isHeaderLine && it.indexOf("probeset_id") != 0 && it.indexOf("AFFX-") != 0){
				probeCount++
				str = it.split("\t")
				for(int i in 1..str.size()-1) {
					if(!celPatientMap[samples[i]].equals(null)){
						def patientNum = celPatientMap[samples[i]]
						sb.append(patientNum + "\t" + patientNum + "\t")
						sb.append(str[0].trim() + "\t")

						// #Calls: -1=NN, 0=AA, 1=AB, 2=BB
						String gt = ""
						if(str[i].indexOf("0") >= 0) gt = " A A"
						if(str[i].indexOf("1") >= 0) gt = " A B"
						if(str[i].indexOf("2") >= 0) gt = " B B"
						if(str[i].indexOf("-1") >= 0) gt = " 0 0"
						sb.append(gt + "\n")

						sbGsm.append(celSampleCdMap[samples[i]] + "\t" + patientNum + "\t")
						sbGsm.append(str[0].trim() + "\t")
						sbGsm.append(gt + "\n")

						//genotypingDataOutputFile.append(sb.toString())
						//sb.delete(0,  sb.length())
					}
				}
				genotypingDataOutputFile.append(sb.toString())
				sb.delete(0,  sb.length())

				gtOutputFileWithGsm.append(sbGsm.toString())
				sbGsm.delete(0,  sbGsm.length())
			}
		}

		log.info ("  Processed probe count:  $probeCount  ")
	}


	File getGenotypingDataOutputFile(){
		File outputFile = new File(outputDirectory + File.separator + studyName + ".lgen")
		/*
		 if(outputFile.size() > 0) {
		 outputFile.delete()
		 outputFile.createNewFile()
		 } */
		return outputFile
	}

	File getGenotypingDataOutputFileWithGsm(){
		File outputFile = new File(outputDirectory + File.separator + studyName + ".lgen.gsm")
		/*
		 if(outputFile.size() > 0) {
		 outputFile.delete()
		 outputFile.createNewFile()
		 }
		 */
		return outputFile
	}

	void setGenotypingFileDirectory(String genotypingFileDirectory){
		this.genotypingFileDirectory = genotypingFileDirectory
	}


	void setSourceGenotypingFilePattern(String sourceGenotypingFilePattern){
		this.sourceGenotypingFilePattern = sourceGenotypingFilePattern
	}

	void setOutputDirectory(String outputDirectory){
		this.outputDirectory = outputDirectory
	}

	void setStudyName(String studyName){
		this.studyName = studyName
	}

	void setCelPatientMap(Map celPatientMap){
		this.celPatientMap = celPatientMap
	}
	
	void setCelSampleCdMap(Map celSampleCdMap){
		this.celSampleCdMap = celSampleCdMap
	}
}
