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

import com.recomdata.pipeline.transmart.SubjectSampleMapping;
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger;

class SampleMapper {

	private static final Logger log = Logger.getLogger(SampleMapper)

	File sampleMappingFile, gsmMappingFile
	String outputDirectory
	Sql deapp
	SubjectSampleMapping subjectSampleMapping
	Map sampleIdMapping

	Map createSampleMappingFromFile(){

		def sampleMap = [:]

		String [] str
		if(sampleMappingFile.exists()){

			log.info("Reading " + sampleMappingFile.toString())

			sampleMappingFile.eachLine{
				str = it.split(",")
				if(str[0].indexOf("experiment_id") == -1)
					sampleMap[str[0].trim()] = str[1].trim().replace(".CEL", "") + ":" + str[2].trim().replace(".CEL", "")
			}
		}else{
			log.warn("Cannot find " + sampleMappingFile.toString())
		}
		return sampleMap
	}


	Map createGsmMappingFromFile(){

		Map gsmMapping = [:]
		Map sampleSubjectMapping = subjectSampleMapping.getSubjectSampleMapping()
		
		Map dataMap = [:]
		sampleIdMapping = [:]
		
		String gsm1 = " ", gsm2 = " ", gsm3 = " "
		String [] str
		if(gsmMappingFile.exists()){

			log.info("Reading " + gsmMappingFile.toString())

			int index = 0
			gsmMappingFile.eachLine{
				if((it.indexOf("study_id") == -1) && (it.indexOf("GPL2004_2005") >= 0)){
					log.info it
					str = it.split("\t")
					if(str.size() != 9){
						log.warn("Line: " + index + " missing column(s) in: " + gsmMappingFile.toString())
						log.info index + ":  " + str.size() + ":  " + it
					} else{
						def patientNum = sampleSubjectMapping[str[2].trim()]
						dataMap["PATIENT_ID"] = patientNum
						dataMap["SAMPLE_ID"] = patientNum
						
						dataMap["TRIAL_NAME"] = str[0].trim()
						dataMap["SUBJECT_ID"] = str[2].trim()
						
						dataMap["SAMPLE_CD"] = str[3].trim()
						dataMap["GPL_ID"] = str[4].trim()
						
						// sample type & sample concept code
						String sampleType = str[5].trim()
						dataMap["SAMPLE_TYPE"] = sampleType
						if(sampleType.indexOf("Normal") == -1) {
							dataMap["CONCEPT_CODE"] = 958943
							dataMap["SAMPLE_TYPE_CD"] = 958943
						}
						else{
							dataMap["CONCEPT_CODE"] = 958942
							dataMap["SAMPLE_TYPE_CD"] = 958943
						}
						
						dataMap["CATEGORY_CD"] = str[8].trim()
						dataMap["SOURCE_CD"] = "STD"
						dataMap["PLATFORM"] = "SNP_profiling"
						dataMap["PLATFORM_CD"] = 958940
						dataMap["ASSAY_ID"] = index
						
						dataMap["DATA_UID"] = dataMap["CONCEPT_CODE"] + "-" + dataMap["PATIENT_ID"]
						
						sampleIdMapping[str[3].trim()] = str[2].trim()
						//subjectSampleMapping.insertSubjectSampleMapping(dataMap)
					}
					index++
					//if(str.size()==4 && it.indexOf(",,") == -1) {
					//	log.info str.size() + ":" + it
					//	gsm1 = str[1].trim()
					//	gsm2 = str[2].trim()
					//	gsm3 = str[3].trim()
					//	gsmMapping[str[0].trim()] = gsm1 + ":" + gsm2 + ":" + gsm3
					//}
				}
			}
		}else{
			log.warn("Cannot find " + gsmMappingFile.toString())
		}

		return gsmMapping
	}
	
	
	Map getSampleIdMapping(){
		return this.sampleIdMapping
	}
	
	
	void setSql(Sql deapp){
		this.deapp = deapp
	}

	
	void setSampleMappingFile(File sampleMappingFile){
		this.sampleMappingFile = sampleMappingFile
	}


	void setGsmMappingFile(File gsmMappingFile){
		this.gsmMappingFile = gsmMappingFile
	}
	
	
	void setSubjectSampleMapping(SubjectSampleMapping subjectSampleMapping){
		this.subjectSampleMapping = subjectSampleMapping
	}
}
