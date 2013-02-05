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

import java.io.File;

import com.recomdata.pipeline.i2b2.PatientDimension
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

class Converter {

	private static final Logger log = Logger.getLogger(Converter)

	static Map subjectPatientMap, sampleCdPatientMap, celExperimentMap, celSampleCdMap
	static Map sampleCdSubjectMap, patientGenderMap, celPatientMap, experimentPatientMap

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		println new Date()
		log.info("Start processing SNP data ...")

		Properties props = Util.loadConfiguration("conf/Converter.properties");

		Sql i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		Converter converter = new Converter()

		log.info "Retrieve subject-patient map  from patient_dimension table ... "
		converter.getSubjectPatientGenderMap(props, i2b2demodata)
		log.info "Print out subject-patient map (subjectPatientMap) ..."
		Util.printMap(subjectPatientMap)
		log.info "Print out patient-gender map (patientGenderMap) ..."
		Util.printMap(patientGenderMap)

		// create subject-sample-patient mapping
		converter.createSubjectSamplePatientMap(props)
		log.info "Print sample_cd-subject map (sampleCdSubjectMap)..."
		Util.printMap(sampleCdSubjectMap)
		
		log.info "Print cel-experiment map (celExperimentMap) ..."
		Util.printMap(celExperimentMap)

		converter.createCelSampleCdMap(props)
		log.info "Print cel-sample_cd map (celSampleCdMap) ..."
		Util.printMap(celSampleCdMap)

		converter.createSampleCdPatientMap()
		log.info("Print sample_cd-patient mapping (sampleCdPatientMap) ...")
		Util.printMap(sampleCdPatientMap)

		converter.createCelPatientMap()
		log.info "Print cel-patient map (celPatientMap) ..."
		Util.printMap(celPatientMap)

		converter.createExperimentPatientMap()
		log.info "Print experiment-patient map (experimentPatientMap) ..."
		Util.printMap(experimentPatientMap)


		// create PLINK .fam file
		converter.createPlinkFamFile(props)

		// reformat Copy Number files
		converter.reformatCopyNumberFile(props)

		// reformat Genotyping data to create .lgen file
		converter.createLongFormatPlinkFile(props)

		// convert PLINK Long-format to Binary format
		converter.createPlinkFile(props)

		//remapPatientNumber(sql)
		//createGPL13314MapFile(sql)
		//refillDeSnpSubjectSortedDef(sql)
	}


	void createCelSampleCdMap(Properties props){

		celSampleCdMap = [:]
		String [] str = []
		File celSampleCdMapping = new File(props.get("source_directory") + File.separator + props.get("cel_sample_cd_mapping"))
		if(celSampleCdMapping.size() > 0){
			celSampleCdMapping.eachLine{
				if(it.toString().toLowerCase().indexOf(".cel") != 0){
					str = it.split("\t")
					celSampleCdMap[str[0].trim()] = str[1].trim()
				}
			}
		}else{
			log.error(celSampleCdMapping.toString() + " is empty ...")
		}
	}


	void createSubjectSamplePatientMap(Properties props){

		// read in subject-sample mapping info and create sample id (GSM#) -> subject_id mapping, such as GSM375420 -> 63T
		File subjectSampleMapping = new File(props.get("source_directory") + File.separator + props.get("subject_sample_mapping"))
		sampleCdSubjectMap = loadSubjectSampleMappingFile(subjectSampleMapping)
	}

	void createCelSamplePatient(Properties props){
		// read in sample_cd -> CEL filename mapping
		File celExperimentMapping = new File(props.get("source_directory") + File.separator + props.get("sample_cel_mapping"))
		loadCelExperimentMapping(celExperimentMapping)
	}


	void createCelPatientMap(){

		celPatientMap = [:]
		celSampleCdMap.each{k, v ->
			println k + "\t" + v + "\t" + sampleCdPatientMap[v]
			if(!sampleCdPatientMap[v].equals(null)) celPatientMap[k] = sampleCdPatientMap[v]
		}
	}


	void createExperimentPatientMap(){

		experimentPatientMap = [:]
		celExperimentMap.each{k, v ->
			if(!celPatientMap[k].equals(null)) experimentPatientMap[v] = celPatientMap[k]
		}
	}


	void createSampleCdPatientMap(){

		sampleCdPatientMap = [:]
		// GSM# -> patient_num mapping, such as GSM248427 -> 1000000239
		sampleCdSubjectMap.each{k, v ->
			sampleCdPatientMap[k] = subjectPatientMap[v]
		}
	}


	void createPlinkFamFile(Properties props){

		File plinkFamFile = new File(props.get("output_directory") + File.separator + props.get("study_name") + ".fam")
		if(plinkFamFile.size() > 0) {
			log.info("Delete/re-create " + plinkFamFile.toString())
			plinkFamFile.delete()
			plinkFamFile.createNewFile()
		}

		log.info "Start creating FAM file: " + plinkFamFile.toString()

		Map famMap = [:]
		sampleCdSubjectMap.each{key, val ->
			famMap[subjectPatientMap[val]] = 1
		}

		StringBuffer sb = new StringBuffer()
		famMap.each{k, v ->
			sb.append(k + "\t" + k + "\t0\t0\t" + patientGenderMap[k] + "\t0\n")
		}

		plinkFamFile.append(sb.toString())

		log.info "End creating FAM file: " + plinkFamFile.toString()
	}


	void reformatCopyNumberFile(Properties props){

		if(props.get("skip_copy_number_process").toString().toLowerCase().equals("yes")){
			log.info "Skip processing Copy Number files ..."
		} else{
			AffymetrixCopyNumberFormatter cnf = new AffymetrixCopyNumberFormatter()
			cnf.setCopyNumberFileDirectory(props.get("source_directory") + "/" + props.get("cn_directory"))
			cnf.setStudyName(props.get("study_name"))
			cnf.setOutputDirectory(props.get("output_directory"))
			cnf.setExperimentPatientMap(experimentPatientMap)
			cnf.setSourceCopyNumberFilePattern(props.get("source_cn_file_pattern"))
			cnf.createCopyNumberFile()
		}
	}


	void createLongFormatPlinkFile(Properties props){

		if(props.get("skip_lgen_file_creation").toString().toLowerCase().equals("yes")){
			log.info "Skip creating PLINK format files ..."
		} else{

			AffymetrixGenotypingDataFormatter gtdf = new AffymetrixGenotypingDataFormatter()
			gtdf.setGenotypingFileDirectory(props.get("source_directory") + File.separator + props.get("gt_directory"))
			gtdf.setStudyName(props.get("study_name"))
			gtdf.setOutputDirectory(props.get("output_directory"))
			gtdf.setSourceGenotypingFilePattern(props.get("source_gt_file_pattern"))
			gtdf.setCelPatientMap(celPatientMap)
			gtdf.setCelSampleCdMap(celSampleCdMap)

			log.info "Creating PLINK format files ..."
			log.info new Date()
			gtdf.createGenotypingFile()
			log.info  new Date()
		}
	}


	void createPlinkFile(Properties props){

		if(props.get("skip_plink_file_creation").toString().toLowerCase().equals("yes")){
			log.info "Skip creating PLINK format files ..."
		} else{
			String outputDir = props.get("output_directory")

			PlinkConverter pc = new PlinkConverter()
			pc.setPlinkSourceDirectory(outputDir)
			pc.setPlinkDestinationDirectory(outputDir)
			pc.setPlink(props.get("plink"))
			pc.setStudyName(props.get("study_name"))

			log.info "Creating Binary PLINK format file ..."
			log.info new Date()
			pc.createBinaryFromLongPlink()
			log.info new Date()

			log.info "Creating PLINK format files for each Chromosome ..."
			pc.recodePlinkFileByChrs()
			log.info new Date()

			log.info "Recoding Binary PLINK format file ..."
			pc.recodePlinkFile()
			log.info new Date()
		}
	}


	void getSubjectPatientGenderMap(Properties props, Sql i2b2demodata){

		PatientDimension pd = new PatientDimension()
		pd.setI2b2demodata(i2b2demodata)
		pd.setSourceSystemPrefix(props.get("source_system_prefix"))
		subjectPatientMap = pd.getPatientSubjectMap()
		patientGenderMap = pd.getPatientGenderMap()
	}


	void createGPL13314MapFile(Sql sql){

		PlinkMapGenerator pmg = new PlinkMapGenerator()
		pmg.setSql(sql)
		File mapFile = new File("C:/Customers/UMich/GPL/GPL13314.map")
		pmg.setMapFile(mapFile)
		pmg.createMapforGPL13314()
	}


	void remapPatientNumber(Sql sql){

		File update = new File("C:/SNP/GSE14860/update_patient_number.sql")
		StringBuffer line = new StringBuffer()

		String qry = "select patient_num, old_patient_num from p"
		sql.eachRow(qry) {
			log.info it.patient_num + "\t" + it.old_patient_num

			line.append "update DE_SNP_DATA_BY_PATIENT set patient_num=" + it.patient_num + " where patient_num = " + it.old_patient_num + ";\n"
			line.append "update DE_SNP_SUBJECT_SORTED_DEF set patient_num=" + it.patient_num + " where patient_num = " + it.old_patient_num + ";\n"
			line.append "update DE_SUBJECT_SNP_DATASET set patient_num=" + it.patient_num + " where patient_num = " + it.old_patient_num + ";\n"
			update.append(line.toString())
		}
	}


	void refillDeSnpSubjectSortedDef(Sql sql){
		File f = new File("C:/Customers/Sanofi/SNP/GSE14860/plink/t")
		String qry = """ insert into de_snp_subject_sorted_def
					  (trial_name, patient_num, patient_position)
					 values(?, ?, ?)"""

		String trial = "GSE14680"

		int index = 1
		f.eachLine{
			println it
			int patientNum = Integer.parseInt(it)
			sql.execute(qry, [trial, patientNum, index])
			index++
		}
	}


	void loadCelExperimentMapping(File celExperimentMapping){

		celExperimentMap = [:]
		String [] str
		if(celExperimentMapping.size() > 0){
			log.info("Start reading CEL-experiment mapping file: " + celExperimentMapping.toString())
			int index = 1
			celExperimentMapping.eachLine{
				if(it.indexOf("experiment_id") == -1){
					if(it.indexOf("\t") != -1) str = it.split("\t")
					else if (it.indexOf(",") != -1) str = it.split(",")
					else str = it.split(" +")

					if(str.size() < 7){
						log.warn("Line: " + index + " missing column(s) in: " + celExperimentMapping.toString())
						log.info index + ":  " + str.size() + ":  " + it
					} else{
						celExperimentMap[str[1].trim()] = str[0].trim()  //+ ":" +  str[5].trim()
						celExperimentMap[str[2].trim()] = str[0].trim()  //+ ":" +  str[5].trim()
					}
				}
				index++
			}
		}else{
			log.error(celExperimentMapping.toString() + " is empty ...")
			throw new RuntimeException(celExperimentMapping.toString() + " is empty ...")
		}

	}


	Map loadSubjectSampleMappingFile(File subjectSampleMapping){

		Map sampleCdSubjectMap = [:]
		String [] str
		if(subjectSampleMapping.size() > 0){
			log.info("Start reading subject-sample mapping file: " + subjectSampleMapping.toString())
			int index = 1
			subjectSampleMapping.eachLine{
				if(it.indexOf("study_id") == -1){
					if(it.indexOf("\t") != -1) str = it.split("\t")
					else str = it.split(" +")

					if(str.size() != 9){
						log.warn("Line: " + index + " missing column(s) in: " + subjectSampleMapping.toString())
						log.info index + ":  " + str.size() + ":  " + it
					} else{
						sampleCdSubjectMap[str[3].trim()] = str[2].trim()
					}
				}
				index++
			}
		}else{
			log.error(subjectSampleMapping.toString() + " is empty ...")
			throw new RuntimeException(subjectSampleMapping.toString() + " is empty ...")
		}

		return sampleCdSubjectMap
	}
}
