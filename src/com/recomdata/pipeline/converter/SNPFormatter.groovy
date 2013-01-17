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
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.recomdata.pipeline.i2b2.PatientDimension
import com.recomdata.pipeline.transmart.SubjectSampleMapping
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator;

class SNPFormatter {

	private static final Logger log = Logger.getLogger(SNPFormatter)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		log.info(new Date())
		log.info("Start processing SNP data ...")

		Properties props = Util.loadConfiguration("conf/SNP.properties");

		Sql i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		SNPFormatter formatter = new SNPFormatter()

		String studyName = props.get("study_name")
		Map samplePatientMap = formatter.getSamplePatientMap(deapp, studyName)
		log.info "Print out sample-patient map from $studyName ..."
		Util.printMap(samplePatientMap)

		formatter.formatCopyNumber(samplePatientMap, props)
		formatter.formatCNCopyNumber(samplePatientMap, props)
		formatter.formatGenotype(samplePatientMap, props)

		PatientDimension pd = new PatientDimension()
		pd.setI2b2demodata(i2b2demodata)
		pd.setSourceSystemPrefix(studyName)

		// create PLINK .fam file
		formatter.createPlinkFamFile(props, pd, samplePatientMap)

		// convert PLINK Long-format to Binary format
		formatter.createPlinkFile(props)

	}


	void formatGenotype(Map samplePatientMap, Properties props){

		if(props.get("skip_reformat_genetype").toString().toLowerCase().equals("yes")){
			log.info "Skip converting genotype file's sample id to patient number ..."
		} else{
			log.info("Start converting genotype file's sample id to patient number ...")

			GenotypeFormatter f = new GenotypeFormatter()
			f.setSamplePatientMap(samplePatientMap)

			File genotype = new File(props.get("output_directory") + File.separator + props.get("study_name") + ".genotype")
			f.setGenotypeFile(genotype)
			f.format()

			log.info("End converting genotype file's sample id to patient number ...")
		}
	}


	void formatCopyNumber(Map samplePatientMap, Properties props){

		if(props.get("skip_reformat_copy_number").toString().toLowerCase().equals("yes")){
			log.info "Skip converting Copy Number file's sample id to patient number ..."
		} else{
			log.info("Start converting Copy Number file's sample id to patient number ...")
			CopyNumberFormatter f = new CopyNumberFormatter()

			f.setSamplePatientMap(samplePatientMap)

			Pattern p = Pattern.compile("chr*\\.cn")
			File srcDir = new File(props.get("output_directory"))
			srcDir.eachFile {
				if(it.toString() =~ /chr.*\.cn/) {
					f.setCopyNumberFile(it)
					f.format()
				}
			}
			log.info("End converting Copy Number file's sample id to patient number ...")
		}
	}


	/**
	 *  replace CN probe's sample id or GSM# with i2b2's patient number
	 *  
	 * @param samplePatientMap
	 * @param props
	 */
	void formatCNCopyNumber(Map samplePatientMap, Properties props){

		if(props.get("skip_reformat_cn_copy_number").toString().toLowerCase().equals("yes")){
			log.info "Skip converting CN Copy Number file's sample id to patient number ..."
		} else{
			log.info("Start converting CN Copy Number file's sample id to patient number ...")
			CopyNumberFormatter f = new CopyNumberFormatter()
			f.setSamplePatientMap(samplePatientMap)

			File cn = new File(props.get("output_directory") + File.separator + props.get("cn_copy_number_output"))
			f.setCopyNumberFile(cn)
			f.format()
			log.info("End converting CN Copy Number file's sample id to patient number ...")
		}
	}


	Map getSamplePatientMap(Sql sql, String studyName){
		SubjectSampleMapping ssm = new SubjectSampleMapping()
		ssm.setDeapp(sql)
		// only extract a map for SNP samples
		Map samplePatientMap = ssm.getSamplePatientMap(studyName, "SNP")
		return samplePatientMap
	}


	/**
	 *  Create *.fam for PLINK to use and its columns are in the following order:
	 *  
	 *    Column 1: Family Id
	 *    Column 2: Individual ID
	 *    Column 3: Paternal ID 
	 *    Column 4: Maternal ID 
	 *    Column 5: Sex ( 1=male; 2=female; other=unknown)
	 *    Column 6: Phenotype
	 *    
	 * @param props
	 */
	void createPlinkFamFile(Properties props, PatientDimension pd, Map samplePatientMap){

		if(props.get("skip_plink_fam").toString().toLowerCase().equals("yes")){
			log.info "Skip creating PLINK FAM file ..."
		} else{

			Map snpPatient = [:]
			samplePatientMap.each{k, v ->
				snpPatient[v] = 1
			}

			File plinkFamFile = new File(props.get("output_directory") + File.separator + props.get("study_name") + ".fam")
			if(plinkFamFile.size() > 0) {
				log.info("Delete the existing FAM file: " + plinkFamFile.toString())
				plinkFamFile.delete()
			}
			log.info("Start creating FAM file: " + plinkFamFile.toString())
			plinkFamFile.createNewFile()

			Map patientGenderMap = pd.getPatientGenderMap()

			StringBuffer sb = new StringBuffer()
			patientGenderMap.each{ k, v ->
				if(snpPatient[k]) sb.append(k + "\t" + k + "\t0\t0\t" + patientGenderMap[k] + "\t0\n")
			}

			plinkFamFile.append(sb.toString())
			sb.setLength(0)

			log.info "End creating FAM file: " + plinkFamFile.toString()
		}
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
