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
  

package com.recomdata.pipeline.loader

import com.recomdata.pipeline.i2b2.ConceptCounts
import com.recomdata.pipeline.i2b2.ConceptDimension
import com.recomdata.pipeline.i2b2.I2b2
import com.recomdata.pipeline.i2b2.I2b2Secure
import com.recomdata.pipeline.i2b2.ObservationFact
import com.recomdata.pipeline.i2b2.PatientDimension
import com.recomdata.pipeline.transmart.BioContent
import com.recomdata.pipeline.transmart.BioContentReference
import com.recomdata.pipeline.transmart.BioContentRepository
import com.recomdata.pipeline.transmart.GplInfo
import com.recomdata.pipeline.transmart.SubjectSampleMapping;
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator


class Loader {

	private static final Logger log = Logger.getLogger(Loader)

	static Map sampleTypes, subjects, platformMap
	static List subjectSamples

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");
		
		println new Date()

		log.info("Start loading property file ...")
		//Properties props = Util.loadConfiguration("loader.properties")
		Properties props = Util.loadConfiguration("conf/SNP.properties")

		Sql i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
		Sql i2b2metadata = Util.createSqlFromPropertyFile(props, "i2b2metadata")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

		File subjectSampleMappingFile = new File(props.get("source_directory") + "/" + props.get("subject_sample_mapping"))

		Loader loader = new Loader()
		loader.loadSubjectSampleMappingFile(subjectSampleMappingFile)

		List concepts = loader.getConcepts(props)
		Map visualAttributes = loader.getVisualAttributes(props)

		// mapping between subject id/sample id and sample type
		//Util.printMap(subjects)
		//Util.printMap(sampleTypes)
		Util.printMap(visualAttributes)

		//loader.deleteConceptPath(props.get("snp_base_node").toString().replace("/", "\\"), i2b2demodata, i2b2metadata, deapp)

		// loading records into PATIENT_DIMENSION
		Map subjectToPatient = loader.loadPatientDimension(props, i2b2demodata)
		Util.printMap(subjectToPatient)

		// loading records into CONCEPT_DIMENSION
		Map conceptPathToCode = loader.loadConceptDimension(props, i2b2demodata, concepts)
		Util.printMap(conceptPathToCode)

		// loading records into I2B2
		loader.loadI2b2(props, i2b2metadata, visualAttributes, conceptPathToCode)

		// loading records into I2B2_SECURE
		loader.loadI2b2Secure(props, i2b2metadata, visualAttributes, conceptPathToCode)

		// loading records into I2B2_TAGS need to be completed
		loader.loadI2b2Tags(props, i2b2metadata)

		// loading records into OBSERVATION_FACT
		loader.loadObservationFact(props, i2b2demodata, conceptPathToCode, subjectToPatient)

		// loading records into CONCEPT_COUNTS
		// TO_BE_DONE: update parent node's count ...
		loader.loadConceptCounts(props, i2b2demodata)

		// loading records into DE_GPL_INFO
		loader.loadDeGPLInfo(props, deapp)

		// loading records into DE_SUBJECT_SAMPLE_MAPPING
		loader.loadDeSubjectSampleMapping(props, deapp, subjectToPatient, conceptPathToCode)

		// loading records into BIO_CONTENT_REPOSITORY
		loader.loadBioContentRepository(props, biomart)

		// loading records into BIO_CONTENT
		loader.loadBioContent(props, biomart)

		// loading records into BIO_CONTENT_REFERENCE
		loader.loadBioContentReference(props, biomart)
	
	}


	// need to be completed
	void loadI2b2Tags(Properties props, Sql i2b2metadata){

	}


	List getConcepts(Properties props){

		String snpBaseNode = props.get("snp_base_node")
		String platformName = props.get("platform_name")

		List concepts = []
		concepts = [snpBaseNode + "/"]
		concepts << snpBaseNode + "/" + platformName + "/"
		sampleTypes.each{k, v ->
			concepts << snpBaseNode + "/" + platformName + "/" + k + "/"
		}
		return concepts
	}


	Map getVisualAttributes(Properties props){

		String snpBaseNode = props.get("snp_base_node")
		String platformName = props.get("platform_name")

		// used to fill I2B2's C_VISUALATTRIBUTES column
		Map visualAttrs = [:]
		visualAttrs[snpBaseNode + "/"] = "FA"
		if(sampleTypes.size() > 0){
			visualAttrs[snpBaseNode + "/" + platformName + "/"] = "FA"
			sampleTypes.each{k, v ->
				// set as high dimensional node
				visualAttrs[snpBaseNode + "/" + platformName + "/" + k + "/"] = "LAH"
			}
		}else{
			// set as high dimensional node
			visualAttrs[snpBaseNode + "/" + platformName + "/"] = "LAH"
		}

		return visualAttrs
	}


	Map loadConceptDimension(Properties props, Sql i2b2demodata, List concepts){

		Map conceptPathToCode = [:]

		ConceptDimension cd = new ConceptDimension()
		cd.setI2b2demodata(i2b2demodata)
		cd.setStudyName(props.get("study_name"))

		if(props.get("skip_concept_dimension").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into CONCEPT_DIMENSION table ..."
		}else{
			log.info "Start loading records into CONCEPT_DIMENSION table ..."
			cd.loadConceptDimensions(concepts)
			log.info "End loading records into CONCEPT_DIMENSION table ..."
		}

		conceptPathToCode = cd.getConceptCode(concepts)

		return conceptPathToCode
	}


	Map loadPatientDimension(Properties props, Sql i2b2demodata){

		PatientDimension pd = new PatientDimension()
		pd.setI2b2demodata(i2b2demodata)
		pd.setSourceSystemPrefix(props.get("source_system_prefix"))

		if(props.get("skip_patient_dimension").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into PATIENT_DIMENSION table ..."
		}else{
			log.info "Start loading records into PATIENT_DIMENSION table ..."

			pd.loadPatientDimensionFromSamples(subjects)
			log.info "End loading records into PATIENT_DIMENSION table ..."
		}

		return pd.getPatientMap()
	}


	void loadI2b2(Properties props, Sql i2b2metadata, Map visualAttrs, Map conceptPathToCode){

		if(props.get("skip_i2b2").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into I2B2 table ..."
		}else{
			log.info "Start loading records into I2B2 table ..."
			I2b2 i2b2 = new I2b2()
			i2b2.setI2b2metadata(i2b2metadata)
			i2b2.setStudyName(props.get("study_name"))
			i2b2.setVisualAttrs(visualAttrs)
			i2b2.loadConceptPaths(conceptPathToCode)
			log.info "End loading records into I2B2 table ..."
		}
	}


	void loadI2b2Secure(Properties props, Sql i2b2metadata, Map visualAttrs, Map conceptPathToCode){

		if(props.get("skip_i2b2_secure").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into I2B2_secure table ..."
		}else{
			log.info "Start loading records into I2B2_secure table ..."
			I2b2Secure i2b2Secure = new I2b2Secure()
			i2b2Secure.setI2b2metadata(i2b2metadata)
			i2b2Secure.setStudyName(props.get("study_name"))
			i2b2Secure.setVisualAttrs(visualAttrs)

			i2b2Secure.loadConceptPaths(conceptPathToCode)
			log.info "End loading records into I2B2_secure table ..."
		}
	}


	void loadObservationFact(Properties props, Sql i2b2demodata, Map conceptPathToCode, Map subjectToPatient){

		if(props.get("skip_observation_fact").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into OBSERVATION_FACT table ..."
		}else{
			log.info "Start loading records into OBSERVATION_FACT table ..."
			ObservationFact obsf = new ObservationFact()
			obsf.setI2b2demodata(i2b2demodata)
			obsf.setConceptPathToCode(conceptPathToCode)
			obsf.setSubjectToPatient(subjectToPatient)
			obsf.setStudyName(props.get("study_name"))
			obsf.setBasePath(props.get("snp_base_node") + "/" + props.get("platform_name") + "/")
			obsf.loadObservationFact(subjects)
			log.info "End loading records into OBSERVATION_FACT table ..."
		}
	}


	void loadConceptCounts(Properties props, Sql i2b2demodata){

		if(props.get("skip_concept_counts").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into CONCEPT_COUNTS table ..."
		}else{
			log.info "Start loading records into CONCEPT_COUNTS table ..."
			ConceptCounts cc = new ConceptCounts()
			cc.setI2b2demodata(i2b2demodata)
			cc.setPlatform(props.get("platform_name"))
			cc.setBasePath(props.get("snp_base_node") + "/")
			cc.setSubjects(subjects)
			cc.loadConceptCounts()
			log.info "End loading records into CONCEPT_COUNTS table ..."
		}
	}


	void loadDeSubjectSampleMapping(Properties props, Sql deapp, Map subjectToPatient, Map conceptPathToCode){

		if(props.get("skip_de_subject_sample_mapping").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into DE_SUBJECT_SAMPLE_MAPPING table ..."
		}else{
			log.info "Start loading records into DE_SUBJECT_SAMPLE_MAPPING table ..."
			SubjectSampleMapping ssm = new SubjectSampleMapping()
			ssm.setDeapp(deapp)
			ssm.setPlatform(props.get("platform_type"))
			ssm.setPlatformPath(props.get("snp_base_node") + "/" + props.get("platform_name") + "/")
			ssm.setSubjectPatientMap(subjectToPatient)
			ssm.setconceptPathToCode(conceptPathToCode)
			ssm.setSubjectSamples(subjectSamples)
			ssm.loadSubjectSampleMapping()
			log.info "End loading records into DE_SUBJECT_SAMPLE_MAPPING table ..."
		}
	}


	void loadDeGPLInfo(Properties props, Sql deapp){

		if(props.get("skip_de_gpl_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into DE_GPL_INFO table ..."
		}else{
			log.info "Start loading records into DE_GPL_INFO table ..."
			GplInfo gi = new GplInfo()
			gi.setDeapp(deapp)
			String gplPlatorm = props.get("platform")
			String title = props.get("title")
			String organism = props.get("organism")
			String markerType = props.get("marker_type")
			gi.insertGplInfo(gplPlatorm, title, organism, markerType)
			log.info "End loading records into DE_GPL_INFO table ..."
		}
	}


	void loadBioContentRepository(Properties props, Sql biomart){

		if(props.get("skip_bio_content_repository").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into BIO_CONTENT_REPOSITORY table ..."
		}else{
			log.info "Start loading records into BIO_CONTENT_REPOSITORY table ..."
			BioContentRepository bcr = new BioContentRepository()
			bcr.setBiomart(biomart)
			bcr.insertBioContentRepository("http://www.genome.jp/", "Y", "KEGG", "URL")
			log.info "End loading records into BIO_CONTENT_REPOSITORY table ..."
		}
	}


	void loadBioContent(Properties props, Sql biomart){

		if(props.get("skip_bio_content").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into BIO_CONTENT table ..."
		}else{
			log.info "Start loading records into BIO_CONTENT table ..."
			BioContent bc = new BioContent()
			bc.setBiomart(biomart)
			bc.loadBioContentForKEGG()
			log.info "End loading records into BIO_CONTENT table ..."
		}
	}


	void loadBioContentReference(Properties props, Sql biomart){

		if(props.get("skip_bio_content_reference").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into BIO_CONTENT_REFERENCE table ..."
		}else{
			log.info "Start loading records into BIO_CONTENT_REFERENCE table ..."
			BioContentReference bcrf = new BioContentReference()
			bcrf.setBiomart(biomart)
			bcrf.loadBioContentReferenceForKEGG()
			log.info "End loading records into BIO_CONTENT_REFERENCE table ..."
		}
	}


	/**
	 *  Using this method to load/process Subject Sample Mapping file, and then populate maps/lists:
	 *  
	 *     subjects:   subject id/sample_id -->  sample type mapping, and will be used for 
	 *     				loading PATIENT_DIMENSION, DOBSERVATION_FACT and CONCEPT_COUNTS
	 *     
	 *     sampleTypes:  list of sample types, and is used to populate CONCEPPT_DIMENSION and I2B2
	 *  
	 * @param subjectSampleMapping		point too SubjectSampleMapping file
	 */

	void loadSubjectSampleMappingFile(File subjectSampleMapping){

		subjects = [:]   	// subject_id -> sampleType mapping
		sampleTypes = [:]	// unique sampleType
		subjectSamples = []
		Map dataMap = [:]

		String [] str
		if(subjectSampleMapping.exists()){
			log.info("Start reading " + subjectSampleMapping.toString())
			int index = 1
			subjectSampleMapping.eachLine{
				//if((it.indexOf("study_id") == -1) && (it.indexOf("Data+SNP_Profiling+PLATFORM+TISSUETYPE") >= 0)){
				//if((it.indexOf("study_id") == -1) && (it.toUpperCase().indexOf("SNP_PROFILING") >= 0)){
				if((it.indexOf("study_id") == -1) && (it.indexOf("subject_id") == -1)) {

					//log.info it

					if(it.indexOf("\t") != -1) str = it.split("\t")
					else str = it.split(" +")

					if(str.size() != 9){
						log.warn("Line: " + index + " missing column(s) in: " + subjectSampleMapping.toString())
						log.info index + ":  " + str.size() + ":  " + it
					} else{
						String sampleType = str[5].trim()

						if(sampleType.size() > 0) {
							sampleTypes[sampleType] = 1
							subjects[str[2].trim()] = sampleType
						}else{
							subjects[str[2].trim()] = ""
						}

						dataMap["TRIAL_NAME"] = str[0].trim()
						dataMap["SITE_ID"] = str[1].trim()
						dataMap["SUBJECT_ID"] = str[2].trim()
						dataMap["SAMPLE_CD"] = str[3].trim()
						dataMap["GPL_ID"] = str[4].trim()
						dataMap["SAMPLE_TYPE"] = sampleType
						dataMap["CATEGORY_CD"] = str[8].trim()

						subjectSamples << dataMap
						dataMap = [:]
					}
				}
				index++
			}
		}else{
			log.error("Cannot find " + subjectSampleMapping.toString())
			throw new RuntimeException("Cannot find " + subjectSampleMapping.toString())
		}
	}


	void deleteConceptPath(String conceptPath, Sql i2b2demodata, Sql i2b2metadata, Sql deapp){

		String qry 
		
		qry = """ delete from de_subject_sample_mapping where concept_code in 
						(select concept_cd from i2b2demodata.concept_dimension where concept_path like '${conceptPath}%') """
		deapp.execute(qry)
		
		qry = "delete from i2b2 where c_fullname like '${conceptPath}%'"
		i2b2metadata.execute(qry)

		qry = "delete from i2b2_secure where c_fullname like '${conceptPath}%'"
		i2b2metadata.execute(qry)

		qry = """ delete from observation_fact where concept_cd in
                        (select concept_cd from concept_dimension where concept_path like '${conceptPath}%') """
		i2b2demodata.execute(qry)

		qry = "delete from concept_dimension where concept_path like '${conceptPath}%'"
		i2b2demodata.execute(qry)

		qry = "delete from concept_counts where concept_path like '${conceptPath}%'"
		i2b2demodata.execute(qry)
	}
}