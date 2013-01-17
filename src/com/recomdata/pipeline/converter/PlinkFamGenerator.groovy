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

import com.recomdata.pipeline.util.Util

import org.apache.log4j.Logger;

class PlinkFamGenerator {

	private static final Logger log = Logger.getLogger(PlinkFamGenerator)

	Map patientMap, samples
	File plinkFamFile
	String outputDirectory, studyName

	/**
	 * create .fam for PLINK to use
	 */
	void createPlinkFamFile(){

		//Util.printMap(patientMap)
		//Util.printMap(samples)
		
		plinkFamFile = getPlinkFamFile()
		if(plinkFamFile.size() > 0) {
			log.info("Delete and re-create " + plinkFamFile.toString())
			plinkFamFile.delete()
			plinkFamFile.createNewFile()
		}

		log.info "Start creating .fam file: " + plinkFamFile.toString()

		if(isPatientUnique()){
			samples.each{key, val ->
				//plinkFamFile.append(key + "\t" + patientMap[key] + "\t0\t0\t0\t-9\n")
				plinkFamFile.append(patientMap[key] + "\t" + patientMap[key] + "\t0\t0\t0\t-9\n")
			}
		}else{
			log.error("Duplicated Patient id fount")
		}
		log.info "End creating .fam file: " + plinkFamFile.toString()
	}


	// TODO: patient uniqueness checking here
	boolean isPatientUnique(){
		return true
	}

	File getPlinkFamFile(){
		return new File(outputDirectory + File.separator + studyName + ".fam")
	}

	void setStudyName(String studyName){
		this.studyName = studyName
	}

	void setOutputDirectory(String outputDirectory){
		this.outputDirectory = outputDirectory
	}

	void setPlinkFamFile(File plinkFamFile){
		this.plinkFamFile = plinkFamFile
	}

	void setPateitMap(Map patientMap){
		this.pateitMap = patientMap
	}

	
	void setSamples(Map samples){
		this.samples = samples
	}
}
