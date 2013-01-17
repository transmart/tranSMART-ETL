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

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import com.recomdata.pipeline.i2b2.PatientDimension;
import com.recomdata.pipeline.transmart.SubjectSampleMapping
import com.recomdata.pipeline.util.Util

import com.recomdata.pipeline.converter.CopyNumberFormatter
import com.recomdata.pipeline.converter.CopyNumberReader

import groovy.sql.Sql;

class PlinkLoader {

	private static final Logger log = Logger.getLogger(PlinkLoader)
	
	// Get the Java runtime
	private Runtime runtime = Runtime.getRuntime();
	
	public static void main(String [] args){

		PropertyConfigurator.configure("conf/log4j.properties");
		
		// extract parameters
		//Properties props = Util.loadConfiguration("PLINK.properties");
		Properties props = Util.loadConfiguration("conf/SNP.properties")

		PlinkLoader pl = new PlinkLoader()

		// check if loading MAP file into de_snp_info and de_snp_probe tables
		if(props.get("skip_load_map_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading annotation data from MAP ..."
		}else{
			//log.info "Start loading annotation data from MAP ..."
			//pl.loadAnnotationData(props)
		}

		// loading SNP data
		pl.load(props)
	}


	// this method is specifically developed for U Mich situation
	void loadAnnotationData(Properties props){
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		File map = new File(props.get("output_directory") + "/all.map")
		if(map.exists()){
			log.info("Satrt loading annotation data from MAP: " + map.toString())
			SnpInfo si = new SnpInfo()
			si.setDeapp(deapp)
			si.loadSnpInfo(map)

			// populate DE_SNP_PROBE
			String qry = """ insert into de_snp_probe(probe_name, snp_id, snp_name)
                             select name, snp_info_id, name from de_snp_info
			                 where name not in (select snp_name from de_snp_probe) """
			deapp.execute(qry)
		}else{
			log.error(map.toString() + "doesn't exist.")
		}
	}


	void load(Properties props){

		log.info(Util.getMemoryUsage(runtime))

		log.info("Loading property file: SNP.properties ...")

		Sql i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		// truncate tables as needed
		//		Util.truncateSNPTable(deapp)
		log.info(new Date())

		// pre-normalize chromosome from letter to number
		preNormalizeChromosomeName(deapp)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		// create a PatientDimension object
		PatientDimension patientDimension = createPatientDimension(props, i2b2demodata)

		// extract the mapping among patient_num, subject_id, concept_code and sample_type from DE_SUBJECT_SAMPLE_MAPPING
		SubjectSampleMapping subjectSampleMapping = createSubjectSampleMapping(deapp)
		// patient_num:concept_code -> subject_id:sample_type map
		Map patientConceptCodeMap = subjectSampleMapping.getPatientConceptCodeMap(props.get("study_name"), props.get("platform_type"))
		//Util.printMap(patientConceptCodeMap)
		Map samplePatientMap = subjectSampleMapping.getSamplePatientMap(props.get("study_name"), props.get("platform_type"))

		// extracted from <study_name>.fam file
		Map patientGenderMap = getPatientGenderMap(props)
		//Util.printMap(patientGenderMap)

		// combine two maps:
		//    1. patientConceptCodeMap:  patient_num:concept_code -> subject_id:sample_type
		//    2. patientGenderMap:  paatient)num -> gender
		// into the one map:
		//     subjectSnpDatasetMap: patient_num:concept_code -> subject_id:sample_type:gender
		// and use it to populate DE_SUBJECT_SNP_DATASET table
		Map subjectSnpDatasetMap = getSubjectSnpDatasetMap(patientConceptCodeMap, patientGenderMap)
		//Util.printMap(subjectSnpDatasetMap)

		// populate DE_SUBJECT_SNP_DATASET and DE_SNP_DATASET_LOC, and also return a map:
		Map patientSnpDatasetMap = loadSubjectSnpDataset(props, deapp, subjectSnpDatasetMap)
		log.info(Util.getMemoryUsage(runtime))
		//Util.printMap(patientSnpDatasetMap)


		// Populate DE_SNP_PROBE_SORTED_DEF
		loadSnpProbeSortedDef(props, deapp)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// Load DE_SNP_SUBJECT_SORTED_DEF
		Map patientSubjectMap = getPatientSubjectMap(patientConceptCodeMap)
		loadSnpSubjectSortedDef(props, deapp, patientSubjectMap)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// Load DE_SNP_COPY_NUMBER
		loadSnpCopyNumber(props, deapp)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// Load DE_SN_CALLS_BY_GSM
		loadSnpCallsByGsm(props, deapp, samplePatientMap)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// a map: patient_num -> dataset_id
		Map patintSnpDatasetMap = getPatientSnpDatasetMap(props, deapp)

		// Read Copy Number data
		CopyNumberReader cnr = new CopyNumberReader()

		// Populate DE_SNP_DATA_BY_PATIENT
		//loadSnpDataByPatient(props, deapp, patientDimension, cnr, subjectSnpDataset)
		loadSnpDataByPatient(props, deapp, patientSnpDatasetMap, cnr)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// Populate DE_SNP_DATA_BY_PROBE
		loadSnpDataByProbe(props, deapp, cnr)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())


		// Normalize Chromosome naming
		postNormalizeChromosomeName(deapp)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

	}



	/**
	 *  create a patient_num:subject_id map for loading DE_SNPSUBJECT_SORTED_DEF 
	 *  
	 * @param patientConceptCodeMap		patient_num:concept_code -> subject_id:sample_type map
	 * @return 							a map with patient_num -> subject_id
	 */
	Map getPatientSubjectMap(Map patientConceptCodeMap){

		Map patientSubjectMap = [:]
		patientConceptCodeMap.each { k, v ->
			patientSubjectMap[k.split(":")[0]] = v.split(":")[0]
		}
		return patientSubjectMap
	}


	Map getSubjectSnpDatasetMap(Map patientConceptCodeMap, Map patientGenderMap){

		Map subjectSnpDatasetMap = [:]
		if(patientGenderMap.size() != patientConceptCodeMap.size()){
			log.error ("Number of patients in PATIENT_DIMENSION didn't match with DE_SUBJECT_SAMPLE_MAPPING's one ...")
		}else{
			patientConceptCodeMap.each{ k, v ->
				String [] key = k.split(":")
				subjectSnpDatasetMap[k] = v + ":" + patientGenderMap[key[0]]
			}
		}

		return subjectSnpDatasetMap
	}


	Map getPatientGenderMap(Properties props){

		Map patientGenderMap = [:]

		File fam = new File(props.get("output_directory") + File.separator + props.get("study_name") + ".fam")

		if(fam.size() > 0){
			fam.eachLine{
				String [] str = it.split("\t")
				if(str[4].equals("1")) patientGenderMap[str[0]] = "M"
				else if(str[4].equals("2")) patientGenderMap[str[0]] = "F"
				else patientGenderMap[str[0]] = "U"
			}
		} else{
			log.error ("PLINK FAM file: " + fam.toString() + " is empty or not exist ... ")
		}

		return patientGenderMap
	}


	Map  loadSubjectSnpDataset(Properties props, Sql deapp, Map subjectSnpDatasetMap){

		Map patientSnpDatasetMap = [:]

		SubjectSnpDataset ssd = new SubjectSnpDataset()
		ssd.setDeapp(deapp)
		ssd.setPlatform(props.get("platform"))
		ssd.setTrialName(props.get("study_name"))

		if(props.get("skip_snp_dataset").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."
		}else{
			log.info "Start loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."

			ssd.loadSubjectSnpDataset(subjectSnpDatasetMap)
			ssd.loadSnpDatasetLocation()

			log.info "End loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."
		}

		Map patientSNpDatasetMap = ssd.getSnpDatasetId(props.get("study_name"))
	}


	//void loadSubjectSnpDataset(Properties props, Sql deapp, Sql i2b2demodata, Map patientConceptCodeMap, PatientDimension patientDimension){
	void loadSubjectSnpDataset(Properties props, SubjectSnpDataset ssd){

		if(props.get("skip_snp_dataset").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."
		}else{
			log.info "Start loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."
			File fam = new File(props.get("output_directory") + "/" + props.get("fam_file_name"))
			ssd.loadSnpDatasetFromFam(fam)
			ssd.loadSnpDatasetLocation()
			log.info "End loading DE_SUBJECT_SNP_DATASET and DE_SNP_DATA_DATASET_LOC ..."
		}
	}


	SubjectSnpDataset createSubjectSnpDataset(Properties props, Sql deapp, Sql i2b2demodata, Map patientConceptCodeMap, PatientDimension patientDimension){

		SubjectSnpDataset ssd = new SubjectSnpDataset()
		ssd.setPatientDimension(patientDimension)
		ssd.setPatientConceptCodeMap(patientConceptCodeMap)
		ssd.setSqlForDeapp(deapp)
		ssd.setSqlForI2b2demodata(i2b2demodata)
		ssd.setPlatform(props.get("platform"))
		ssd.setTrialName(props.get("study_name"))
		ssd.setSourceSystemPrefix(props.get("source_system_prefix"))

		return ssd
	}


	Map getPatientSnpDatasetMap(Properties props, Sql deapp){

		Map patientSnpDatasetMap = [:]

		SubjectSnpDataset ssd = new SubjectSnpDataset()
		ssd.setSqlForDeapp(deapp)
		ssd.setTrialName(props.get("study_name"))
		patientSnpDatasetMap  = ssd.getSnpDatasetId()

		return patientSnpDatasetMap
	}


	void loadSnpProbeSortedDef(Properties props, Sql deapp){

		if(props.get("skip_snp_probe_sorted_def").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_PROBE_SORTED_DEF ..."
		}else{
			log.info "Start loading DE_SNP_PROBE_SORTED_DEF ..."

			SnpProbeSortedDef spsd = new SnpProbeSortedDef();
			spsd.setMapDirectory(props.get("output_directory"))
			spsd.setSqlForDeapp(deapp)
			spsd.setPlatform(props.get("platform"))

			// insert a record for each chromosome
			spsd.loadSnpDefByChromosomes(Util.getChromosomeList())

			// insert a record for all chromosomes
			def allProbes = spsd.getSnpProbeDefByChr("all")
			spsd.loadSnpDefByChr("ALL", allProbes['total'], allProbes['snpDef'].toString())

			log.info "End loading DE_SNP_PROBE_SORTED_DEF ..."
		}
	}


	PatientDimension createPatientDimension(Properties props, Sql i2b2demodata){

		PatientDimension patientDimension = new PatientDimension()
		patientDimension.setSqlForI2b2demodata(i2b2demodata)
		patientDimension.setSourceSystemPrefix(props.get("source_system_prefix"))

		return patientDimension
	}


	void loadSnpDataByPatient(Properties props, Sql deapp, Map patientSnpDatasetMap, CopyNumberReader cnr){

		if(props.get("skip_snp_data_by_patient").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_DATA_BY_PATIENT ..."
		}else{
			int start_chr = Integer.parseInt(props.get("start_chr"))
			int end_chr = Integer.parseInt(props.get("end_chr"))

			SnpDataByPatient sdbp = new SnpDataByPatient()
			sdbp.setSubjectSnpDatasetMap(patientSnpDatasetMap)
			sdbp.setCopyNumberReader(cnr)
			sdbp.setDeapp(deapp)
			sdbp.setTrial(props.get("study_name"))
			sdbp.setPrefix(props.get("chromosome_prefix"))
			sdbp.setPath(props.get("output_directory"))
			sdbp.setSourceSystemPrefix(props.get("source_system_prefix"))

			for (i in start_chr..end_chr){
				def chr = i.toString()
				sdbp.loadSnpDataByPatientChromosome(chr)
			}
			sdbp.loadSnpDataByPatientAllChromosomes()
		}
	}



	void loadSnpDataByProbe(Properties props, Sql deapp, CopyNumberReader cnr){

		if(props.get("skip_snp_data_by_probe").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_DATA_BY_PROBE ..."
		} else{

			int start_chr = Integer.parseInt(props.get("start_chr"))
			int end_chr = Integer.parseInt(props.get("end_chr"))

			SnpDataByProbe sdbProbe = new  SnpDataByProbe()
			sdbProbe.setTrial(props.get("study_name"))
			sdbProbe.setPath(props.get("output_directory"))
			sdbProbe.setPrefix(props.get("chromosome_prefix"))
			sdbProbe.setDeapp(deapp)
			sdbProbe.setCut(props.get("cut"))
			sdbProbe.setCopyNumberReader(cnr)

			for (i in start_chr..end_chr){
				def chr = i.toString()
				sdbProbe.loadSnpDataByProbeChromsome(chr)
			}
		}
	}


	void loadSnpCopyNumber(Properties props, Sql deapp){

		if(props.get("skip_snp_copy_number").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_COPY_NUMBER ..."
		} else{

			int start_chr = Integer.parseInt(props.get("start_chr"))
			int end_chr = Integer.parseInt(props.get("end_chr"))

			SnpCopyNumber scn = new  SnpCopyNumber()
			scn.setDeapp(deapp)

			for (i in start_chr..end_chr){
				def chr = i.toString()

				// loading Copy Number files one-by-one
				File copyNumberFile = new File(props.get("output_directory") + "/" + props.get("chromosome_prefix") + chr + ".cn")
				scn.setCopyNumberFile(copyNumberFile)

				int batchSize =  Integer.parseInt(props.get("buffer_size"))
				scn.loadSnpCopyNumber(batchSize)
			}
		}
	}


	void loadSnpCallsByGsm(Properties props, Sql deapp, Map samplePatientMap){

		if(props.get("skip_snp_calls_by_gsm").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_CALLS_BY_GSM ..."
		} else{

			SnpCallsByGsm scbg = new  SnpCallsByGsm()
			scbg.setSql(deapp)
			scbg.setPatientSampleMap(samplePatientMap)

			// loading the genotype file and the default name is <study_name>.genotype
			File genotypeFile = new File(props.get("output_directory") + "/" + props.get("study_name") + ".genotype")
			scbg.setGenotypeFile(genotypeFile)

			int batchSize =  Integer.parseInt(props.get("buffer_size"))
			scbg.loadSnpCallsByGsm(batchSize)
		}
	}


	void loadSnpSubjectSortedDef(Properties props, Sql deapp, Map patientSubjectMap){

		if(props.get("skip_snp_subject_sorted_def").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_SUBJECT_SORTED_DEF ..."
		} else{
			SnpSubjectSortedDef sssd = new SnpSubjectSortedDef()
			sssd.setDeapp(deapp)
			sssd.setTrialName(props.get("study_name"))

			// use one PED file's first column to populate DE_SNP_SUBJECT_SORTED_DEF table,
			// this PED file can be from any chromsome
			File pedFile = new File(props.get("output_directory") + "/" + props.get("chromosome_prefix") + props.get("start_chr") + ".ped")
			sssd.setPedFile(pedFile)

			sssd.setPatientSubjectMap(patientSubjectMap)
			sssd.loadSubjectSorteDef()
		}
	}




	void preNormalizeChromosomeName(Sql deapp){

		log.info("Start renameing chromosomes from letter to number, such as X to 23, ...")

		List <String> chrMap = [
			"23:X",
			"24:Y",
			"25:XY",
			"26:MT"
		]
		Util.normalizeChromosomeNaming(deapp, "de_snp_info", "chrom", chrMap)
		Util.normalizeChromosomeNaming(deapp, "de_snp_data_by_patient", "chrom", chrMap)
		Util.normalizeChromosomeNaming(deapp, "de_snp_probe_sorted_def", "chrom", chrMap)

		log.info("End renameing chromosomes from X, Y, XY and MT to 23, 24, 25 and 26 ...")
	}


	void postNormalizeChromosomeName(Sql deapp){

		log.info("Start renameing chromosomes from 23, 24, 25 and 26 to X, Y, XY and MT ...")

		List <String> chrMap = [
			"X:23",
			"Y:24",
			"XY:25",
			"MT:26"
		]
		Util.normalizeChromosomeNaming(deapp, "de_snp_info", "chrom", chrMap)
		Util.normalizeChromosomeNaming(deapp, "de_snp_data_by_patient", "chrom", chrMap)
		Util.normalizeChromosomeNaming(deapp, "de_snp_probe_sorted_def", "chrom", chrMap)

		log.info("End renameing chromosomes from 23, 24, 25 and 26 to X, Y, XY and MT ...")
	}


	SubjectSampleMapping createSubjectSampleMapping(Sql deapp){
		log.info("Create a SubjectSampleMapping object ... ")
		SubjectSampleMapping subjectSampleMapping = new SubjectSampleMapping()
		subjectSampleMapping.setDeapp(deapp)
		return subjectSampleMapping
	}
}
