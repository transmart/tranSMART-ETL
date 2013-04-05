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
  

package com.recomdata.pipeline.omicsoft

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator

import com.recomdata.pipeline.transmart.BioAssayAnalysis
import com.recomdata.pipeline.transmart.BioAssayAnalysisData
import com.recomdata.pipeline.transmart.BioAssayAnalysisDataTea
import com.recomdata.pipeline.transmart.BioAssayAnalysisPlatform
import com.recomdata.pipeline.transmart.BioAssayDataAnnotation
import com.recomdata.pipeline.transmart.BioAssayDataset
import com.recomdata.pipeline.transmart.BioAssayFeatureGroup
import com.recomdata.pipeline.transmart.BioAssayPlatform
import com.recomdata.pipeline.transmart.BioContent
import com.recomdata.pipeline.transmart.BioContentReference
import com.recomdata.pipeline.transmart.BioContentRepository
import com.recomdata.pipeline.transmart.BioDataCompound
import com.recomdata.pipeline.transmart.BioDataDisease
import com.recomdata.pipeline.transmart.BioDataOmicMarker
import com.recomdata.pipeline.transmart.BioDataUid
import com.recomdata.pipeline.transmart.BioExperiment
import com.recomdata.pipeline.transmart.SearchKeyword
import com.recomdata.pipeline.transmart.SearchKeywordTerm
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql


class OmicsoftLoader {

	private static final Logger log = Logger.getLogger(OmicsoftLoader)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		log.info ("Start loading ...")

		Util util = new Util()

		Properties props = Util.loadConfiguration("conf/Omicsoft.properties")

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")


		OmicsoftLoader ol = new OmicsoftLoader()

		// loading Omicsoft's project and tests data files
		ol.prepare(props, biomart)

		if(!props.get("skip_load_raw_data").toString().toLowerCase().equals("yes")){
			ol.loadRawData(props, biomart)
		}

		ol.createIndexes(props, biomart)


		ol.loadBioExperiemnt(props, biomart)
		ol.loadBioAssayPlatform(props, biomart)
		ol.loadBioAssayFeatureGroup(props, biomart)
		ol.loadBioAssayAnalysisPlatform(props, biomart)
		ol.loadBioAssayAnalysis(props, biomart)

		if(!props.get("skip_create_gse_analysis").toString().toLowerCase().equals("yes")){
			ol.createGseAnalysisTable(props, biomart)
		}

		if(!props.get("skip_create_assay_analysis_data").toString().toLowerCase().equals("yes")){
			ol.createAssayAnalysisDataTable(props, biomart)
		}

		ol.loadBioAssayDataset(props, biomart)
		ol.loadBioDataUid(props, biomart)

		ol.loadBioAssayAnalysisData(props, biomart)
		ol.loadBioAssayAnalysisDataTea(props, biomart)
		ol.loadBioAssayDataAnnotation(props, biomart)


		ol.loadBioContentRepository(props, biomart)
		ol.loadBioContent(props, biomart)
		ol.loadBioContentReference(props, biomart)

		ol.loadBioDataOmicMarker(props, biomart)

		ol.loadSearchKeyword(props, searchapp)
		ol.loadSearchKeywordTerm(props, searchapp)

		ol.updateBioAssayAnalysisDataCount(props, biomart)
		ol.updateBioAssayAnalysisTeaDataCount(props, biomart)

		ol.loadBioDataDisease(props, biomart)
		ol.loadBioDataCompound(props, biomart)
	}


	void loadRawData(Properties props, Sql biomart){

		File testsDataDirectory = new File(props.get("tests_data_directory"))

		if(testsDataDirectory.isDirectory()){

			testsDataDirectory.eachFile {

				String fileName = it.name

				if(fileName.indexOf(props.get("tests_data_suffix")) != -1){

					log.info "Start looking for project info file for ${it.toString()} ..."

					String gseName = fileName.split(/ +/)[0]
					String platform = fileName.split(/ +/)[1].split(/\./)[0]

					File projectInfo = new File(props.get("project_info_directory") + File.separator + gseName + props.get("project_info_suffix"))
					if(projectInfo.exists()){
						if(!isStudyLoaded(biomart, gseName, platform)){
							log.info "Start loading Study $gseName ... "
							loadTestsData(props, biomart, it)

							if(isProjectInfoLoaded(biomart, gseName, props.get("project_info_table"))){
								log.info "Project Info for Study $gseName is loaded already ... "
							}else{
								loadProjectInfo(props, biomart, projectInfo)
							}
						} else{
							log.info "Study $gseName is loaded already ... "
						}
					} else{
						projectInfo = new File(props.get("project_info_directory") + File.separator + gseName.toLowerCase() + props.get("project_info_suffix"))
						if(projectInfo.exists()){
							if(!isStudyLoaded(biomart, gseName, platform)){
								log.info "Start loading Study $gseName ... "
								loadTestsData(props, biomart, it)
								
								if(isProjectInfoLoaded(biomart, gseName, props.get("project_info_table"))){
									log.info "Project Info for Study $gseName is loaded already ... "
								}else{
									loadProjectInfo(props, biomart, projectInfo)
								}
							} else{
								log.info "Study $gseName is loaded already ... "
							}
						} else{
							log.error(projectInfo.toString() + " doen't exist." )
						}
					}
				}
			}
		} else{
			log.error("There is no such directory: " + testsDataDirectory.toString())
		}
	}


	void loadAnnotation(Sql biomart, String annotationName){
	}


	boolean isAnnotationLoaded(Sql biomart, String annotationName){

		log.info "Check if $annotationName is loaded already ... "

		String qry = """ select count(*) from bio_assay_analysis_data t1, bio_experiment t2
						 where t1.bio_experiment_id=t2.bio_experiment_id and t2.accession=?"""
		if(biomart.firstRow(qry, [annotationName])[0] > 0)  return true
		else return false
	}


	boolean isProjectInfoLoaded(Sql biomart, String gseName, String projectInfotable){

		log.info "Check if $gseName is loaded into $projectInfotable already ... "

		String qry = """ select count(*) from $projectInfotable	where name=? """
		if(biomart.firstRow(qry, [gseName])[0] > 0)  return true
		else return false
	}


	boolean isStudyLoaded(Sql biomart, String gseName){

		log.info "Check if $gseName is loaded already ... "

		String qry = """ select count(*) from bio_assay_analysis_data t1, bio_experiment t2
					     where t1.bio_experiment_id=t2.bio_experiment_id and t2.accession=?"""
		if(biomart.firstRow(qry, [gseName])[0] > 0)  return true
		else return false
	}



	boolean isStudyLoaded(Sql biomart, String gseName, String platform){

		log.info "Check if $gseName is loaded already ... "

		String qry = """ select count(*) from bio_assay_analysis_data t1, bio_experiment t2, bio_assay_platform t3
							 where t1.bio_experiment_id=t2.bio_experiment_id and t2.accession=? 
								   and t1.bio_assay_platform_id=t3.bio_assay_platform_id and t3.platform_name=?"""
		if(biomart.firstRow(qry, [gseName, platform])[0] > 0)  return true
		else return false
	}


	void prepare(Properties props, Sql biomart){

		// prepare table for PROJECT meta data
		if(props.get("truncate_project_info_table").toString().toLowerCase().equals("yes")){
			Util.truncateProjectInfoTable(biomart, props.get("project_info_table"))
		}

		if(props.get("recreate_project_info_table").toString().toLowerCase().equals("yes")){
			Util.createProjectInfoTable(biomart, props.get("project_info_table"))
		}


		// prepare table for TESTS data
		if(props.get("truncate_tests_table").toString().toLowerCase().equals("yes")){
			Util.truncateTestsTable(biomart, props.get("tests_data_table"))
		}

		if(props.get("recreate_tests_table").toString().toLowerCase().equals("yes")){
			Util.createTestsTable(biomart, props.get("tests_data_table"))
		}
	}


	void createIndexes(Properties props, Sql biomart){

		if(props.get("recreate_tests_index").toString().toLowerCase().equals("yes")){
			log.info "Create index(es) for ${props.get("tests_data_table")} on  (platform, test) ... "
			String qry = " create index idx_tests on ${props.get("tests_data_table")} (platform, test) nologging"
			biomart.execute(qry)
		}else{
			log.info "Skip creating index(es) for ${props.get("tests_data_table")} on  (platform, test) ... "
		}
	}


	/**
	 * Load study's meta data from PROJECT or PROJECT INFO  file into a temporary table
	 * 
	 * @param props		Properties object reated from the property file Omicsoft.properties
	 * @param biomart	Sql object 
	 */
	void loadProjectInfo(Properties props, Sql biomart){

		File projectInfoSourceDirectory = new File(props.get("project_info_src_directory"))
		loadProjectInfo(props, biomart, projectInfoSourceDirectory)

		/*
		 if(props.get("skip_project_info").toString().toLowerCase().equals("yes")){
		 log.info "Skip loading PROJECT_INFO data ..."
		 }else{
		 File projectInfoSourceDirectory = new File(props.get("project_info_directory"))
		 ProjectInfo prj = new ProjectInfo()
		 prj.setSql(biomart)
		 prj.setProjectInfoTable(props.get("project_info_table"))
		 prj.setProjectInfoSuffix(props.get("project_info_suffix"))
		 prj.loadProjectInfo(projectInfoSourceDirectory)
		 }
		 */
	}


	void loadProjectInfo(Properties props, Sql biomart, File projectInfo){

		if(props.get("skip_project_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading PROJECT_INFO data ..."
		}else{

			File projectInfoSourceDirectory = new File(props.get("project_info_directory"))
			ProjectInfo prj = new ProjectInfo()

			prj.setSql(biomart)
			prj.setProjectInfoTable(props.get("project_info_table"))
			prj.setProjectInfoSuffix(props.get("project_info_suffix"))

			prj.loadProjectInfo(projectInfo)
		}
	}

	/**
	 *  Load Tests data into a temporary table
	 *  
	 * @param props		Properties object reated from the property file Omicsoft.properties
	 * @param biomart	Sql object 
	 */
	void loadTestsData(Properties props, Sql biomart){

		File dataDir = new File(props.get("tests_data_directory"))
		loadTestsData(props, biomart, dataDir)

		/*
		 if(props.get("skip_tests_data").toString().toLowerCase().equals("yes")){
		 log.info "Skip loading Tests data ..."
		 }else{
		 File dataDir = new File(props.get("tests_data_directory"))
		 TestsData td = new TestsData()
		 td.setSql(biomart)
		 td.setTableName(props.get("tests_data_table"))
		 td.setSuffix(props.get("tests_data_suffix"))
		 if(props.get("truncate_tests").toString().toLowerCase().equals("yes")){
		 //td.truncateTests()
		 td.createTestsTable()
		 }
		 td.loadTestsDataDirectory(dataDir)
		 }
		 */
	}


	void loadTestsData(Properties props, Sql biomart, File input){

		if(props.get("skip_tests_data").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Tests data ..."
		}else{

			TestsData td = new TestsData()

			td.setSql(biomart)
			td.setTableName(props.get("tests_data_table"))
			td.setSuffix(props.get("tests_data_suffix"))

			td.loadTestsDataDirectory(input)
		}
	}



	void loadBioExperiemnt(Properties props, Sql biomart){

		if(props.get("skip_bio_experiment").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_EXPERIMENT  ..."
		}else{

			String projectInfoTable = props.get("project_info_table")

			BioExperiment be = new BioExperiment()
			be.setBiomart(biomart)
			be.setProjectInfoTable(projectInfoTable)
			be.loadBioExperiments()
		}
	}


	void loadBioAssayPlatform(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_platform").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_PLATFORM  ..."
		}else{

			String projectInfoTable = props.get("project_info_table")

			BioAssayPlatform ap = new BioAssayPlatform()
			ap.setBiomart(biomart)
			ap.setProjectInfoTable(projectInfoTable)
			ap.loadBioAssayPlatforms()
		}
	}


	void loadBioAssayFeatureGroup(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_feature_group").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into Bio_Assay_Feature_Group  ..."
		}else{

			String testsDataTable = props.get("tests_data_table")

			BioAssayFeatureGroup afg = new BioAssayFeatureGroup()
			afg.setBiomart(biomart)
			afg.setTestsDataTable(testsDataTable)
			afg.loadBioAssayFeatureGroup()
		}
	}



	void loadBioAssayDataAnnotation(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_data_annotation").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_DATA_ANNOTATION  ..."
		}else{

			String testsDataTable = props.get("tests_data_table")
			String gxAnnotationTable = props.get("gx_annotation_table")

			BioAssayDataAnnotation ada = new BioAssayDataAnnotation()
			ada.setBiomart(biomart)
			ada.setTestsDataTable(testsDataTable)
			ada.setGxAnnotationTable(gxAnnotationTable)

			ada.loadBioAssayDataAnnotation()
		}
	}



	void loadBioAssayAnalysisPlatform(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis_platform").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASY_ANALYSIS_PLTFM  ..."
		}else{

			String testsDataTable = props.get("tests_data_table")

			BioAssayAnalysisPlatform aap = new BioAssayAnalysisPlatform()
			aap.setBiomart(biomart)
			aap.loadBioAssayAnalysisPlatform(props.get("assay_analysis_platform_name"))
		}
	}



	void loadBioAssayAnalysis(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_ANALYSIS ..."
		}else{

			String testsDataTable = props.get("tests_data_table")

			BioAssayAnalysisPlatform aap = new BioAssayAnalysisPlatform()
			aap.setBiomart(biomart)
			long bioAssayAnalysisPlatformId = aap.getBioAssayAnalysisPlatformId(props.get("assay_analysis_platform_name"))

			BioAssayAnalysis baa = new BioAssayAnalysis()
			baa.setBiomart(biomart)
			baa.setBioAssayAnalysisPlatformId(bioAssayAnalysisPlatformId)

			String qry = """ select distinct name, test from $testsDataTable """
			biomart.eachRow(qry) {
				baa.loadBioAssayAnalysis(it.test, it.name)
			}
		}
	}


	void updateBioAssayAnalysisTeaDataCount(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_ANALYSIS ..."
		}else{

			log.info("Start updating BIO_ASSAY_ANALYSIS's TEA_DATA_COUNT ...")

			BioAssayAnalysis baa = new BioAssayAnalysis()
			baa.setBiomart(biomart)

			String qry = """ select distinct t1.bio_assay_analysis_id, count(distinct t3.bio_marker_id) as tea_count  
							 from ASSAY_ANALYSIS_DATA t1, BIO_DATA_OMIC_MARKER t2, BIO_MARKER t3 
							 where t1.BIO_ASY_ANALYSIS_DATA_ID = t2.BIO_DATA_ID 
									and	t3.BIO_MARKER_ID=t2.BIO_MARKER_ID 
									and t1.TEA_NORMALIZED_PVALUE <= 0.05
							 group by t1.BIO_ASSAY_ANALYSIS_ID """
			biomart.eachRow(qry) {
				baa.updateTeaDataCount((long) it.bio_assay_analysis_id, (int) it.tea_count)
			}

			log.info("End updating BIO_ASSAY_ANALYSIS's TEA_DATA_COUNT ...")
		}
	}


	void updateBioAssayAnalysisDataCount(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_ANALYSIS ..."
		}else{

			log.info("Start updating BIO_ASSAY_ANALYSIS's DATA_COUNT ...")

			BioAssayAnalysis baa = new BioAssayAnalysis()
			baa.setBiomart(biomart)

			String qry = """ select distinct t1.bio_assay_analysis_id, count(distinct t3.bio_marker_id) as tea_count
							 from ASSAY_ANALYSIS_DATA t1, BIO_DATA_OMIC_MARKER t2, BIO_MARKER t3
							 where t1.BIO_ASY_ANALYSIS_DATA_ID = t2.BIO_DATA_ID
									and	t3.BIO_MARKER_ID=t2.BIO_MARKER_ID 
									and (t1.fold_change_ratio >=1.0 or t1.fold_change_ratio<=-1.0) 
									and (t1.preferred_pvalue is null or t1.preferred_pvalue <=0.1)
							 group by t1.BIO_ASSAY_ANALYSIS_ID """
			biomart.eachRow(qry) {
				baa.updateDataCount((long) it.bio_assay_analysis_id, (int) it.tea_count)
			}

			log.info("End updating BIO_ASSAY_ANALYSIS's DATA_COUNT ...")
		}
	}


	void loadBioAssayDataset(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_dataset").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_DATASET ..."
		}else{

			String testsDataTable = props.get("tests_data_table")

			BioAssayDataset bad = new BioAssayDataset()
			bad.setBiomart(biomart)
			bad.setTestsDataTable(testsDataTable)
			bad.loadBioAssayDataset()
		}
	}


	void loadBioAssayAnalysisData(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis_data").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_ANALYSIS_DATA ..."
		}else{

			BioAssayAnalysisPlatform aap = new BioAssayAnalysisPlatform()
			aap.setBiomart(biomart)
			int bioAssayAnalysisPlatformId = aap.getBioAssayAnalysisPlatformId(props.get("assay_analysis_platform_name"))

			String testsDataTable = props.get("tests_data_table")

			BioAssayAnalysisData baad = new BioAssayAnalysisData()
			baad.setBiomart(biomart)
			baad.setTestsDataTable(testsDataTable)

			if(props.get("drop_bio_assay_analysis_data_index").toString().toLowerCase().equals("yes")){
				log.info "Start dropping indexes from BIO_ASSAY_ANALYSIS_DATA ..."
				baad.dropBioAssayAnalysisDataIndexes()
				log.info "End dropping indexes from BIO_ASSAY_ANALYSIS_DATA ..."
			}else{
				log.info "Skip dropping indexes for BIO_ASSAY_ANALYSIS_DATA ..."
			}

			if(props.get("disable_bio_assay_analysis_data_constraint").toString().toLowerCase().equals("yes")){
				log.info "Start disabling constraints from BIO_ASSAY_ANALYSIS_DATA ..."
				baad.disableBioAssayAnalysisDataConstraints()
				log.info "End disabling constraints from BIO_ASSAY_ANALYSIS_DATA ..."
			}else{
				log.info "Skip disabling constraints for BIO_ASSAY_ANALYSIS_DATA ..."
			}

			baad.loadBioAssayAnalysisData()

			if(props.get("create_bio_assay_analysis_data_index").toString().toLowerCase().equals("yes")){
				log.info "Start creating indexes from BIO_ASSAY_ANALYSIS_DATA ..."
				baad.createBioAssayAnalysisDataIndexes()
				log.info "Start creating indexes from BIO_ASSAY_ANALYSIS_DATA ..."
			}else{
				log.info "Skip creating indexes for BIO_ASSAY_ANALYSIS_DATA ..."
			}

			if(props.get("enable_bio_assay_analysis_data_constraint").toString().toLowerCase().equals("yes")){
				log.info "Start enabling constraints from BIO_ASSAY_ANALYSIS_DATA ..."
				baad.enableBioAssayAnalysisDataConstraints()
				log.info "End enabling constraints from BIO_ASSAY_ANALYSIS_DATA ..."
			}else{
				log.info "Skip enabling constraints for BIO_ASSAY_ANALYSIS_DATA ..."
			}
		}
	}


	void loadBioAssayAnalysisDataTea(Properties props, Sql biomart){

		if(props.get("skip_bio_assay_analysis_data_tea").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_ASSAY_ANALYSIS_DATA_TEA ..."
		}else{

			BioAssayAnalysisPlatform aap = new BioAssayAnalysisPlatform()
			aap.setBiomart(biomart)
			int bioAssayAnalysisPlatformId = aap.getBioAssayAnalysisPlatformId(props.get("assay_analysis_platform_name"))

			String testsDataTable = props.get("tests_data_table")

			BioAssayAnalysisDataTea baadt = new BioAssayAnalysisDataTea()
			baadt.setBiomart(biomart)
			baadt.setTestsDataTable(testsDataTable)

			if(props.get("drop_bio_assay_analysis_data_tea_index").toString().toLowerCase().equals("yes")){
				log.info "Start dropping indexes from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
				baadt.dropBioAssayAnalysisDataTeaIndexes()
				log.info "End dropping indexes from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}else{
				log.info "Skip dropping indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}

			if(props.get("disable_bio_assay_analysis_data_tea_constraint").toString().toLowerCase().equals("yes")){
				log.info "Start disabling constraints from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
				baadt.disableBioAssayAnalysisDataTeaConstraints()
				log.info "End dropping indexes from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}else{
				log.info "Skip disabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}

			baadt.loadBioAssayAnalysisDataTea()

			if(props.get("create_bio_assay_analysis_data_tea_index").toString().toLowerCase().equals("yes")){
				log.info "Start creating indexes from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
				baadt.createBioAssayAnalysisDataTeaIndexes()
				log.info "End creating indexes from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}else{
				log.info "Skip creating indexes for BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}

			if(props.get("enable_bio_assay_analysis_data_tea_constraint").toString().toLowerCase().equals("yes")){
				log.info "Start enabling constraints from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
				baadt.enableBioAssayAnalysisDataTeaConstraints()
				log.info "Start enabling constraints from BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}else{
				log.info "Skip enabling constraints for BIO_ASSAY_ANALYSIS_DATA_TEA ..."
			}
		}
	}


	void loadBioDataUid(Properties props, Sql biomart){

		if(props.get("skip_bio_data_uid").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_DATA_UID ..."
		}else{
			BioDataUid bdu = new BioDataUid()
			bdu.setBiomart(biomart)
			bdu.loadBioDataUid()
		}
	}



	void loadSearchKeyword(Properties props, Sql searchapp){

		if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into Search_Keyword ..."
		}else{
			SearchKeyword sk = new SearchKeyword()
			sk.setSearchapp(searchapp)
			sk.loadOmicsoftGSESearchKeyword(props.get("biomart_username"))
			sk.loadOmicsoftDiseaseSearchKeyword(props.get("biomart_username"))
			sk.loadOmicsoftCompoundSearchKeyword(props.get("biomart_username"))
		}
	}


	void loadSearchKeywordTerm(Properties props, Sql searchapp){

		if(props.get("skip_search_keyword_term").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into Search_Keyword_Term ..."
		}else{
			SearchKeywordTerm skt = new SearchKeywordTerm()
			skt.setSearchapp(searchapp)
			skt.loadSearchKeywordTerm()
		}
	}


	void loadBioContentRepository(Properties props, Sql biomart){

		if(props.get("skip_bio_content_repository").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_CONTENT_REPOSITORY ..."
		}else{
			BioContentRepository bcr = new BioContentRepository()
			bcr.setBiomart(biomart)
			bcr.insertBioContentRepository('http://www.ncbi.nlm.nih.gov/', 'Y', 'NCBI', 'URL')
		}
	}


	void loadBioContent(Properties props, Sql biomart){

		if(props.get("skip_bio_content").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_CONTENT ..."
		}else{

			BioContentRepository bcr = new BioContentRepository()
			bcr.setBiomart(biomart)
			long bioContentRepositoryId = bcr.getBioContentRepositoryId('http://www.ncbi.nlm.nih.gov/', 'NCBI')

			BioContent bc = new BioContent()
			bc.setBiomart(biomart)

			String qry = "select distinct name from gse_analysis"
			biomart.eachRow(qry) {
				String location = "geo/query/acc.cgi?acc=" + it.name
				String fileType = "Experiment Web Link"
				bc.insertBioContent(bioContentRepositoryId, location , fileType, it.name)
			}
		}
	}



	void loadBioContentReference(Properties props, Sql biomart){

		if(props.get("skip_bio_content_reference").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_CONTENT_REFERENCE ..."
		}else{

			BioContentRepository bcrp = new BioContentRepository()
			bcrp.setBiomart(biomart)
			long bioContentRepositoryId = bcrp.getBioContentRepositoryId('http://www.ncbi.nlm.nih.gov/', 'NCBI')

			BioContentReference bcrf = new BioContentReference()
			bcrf.setBiomart(biomart)

			String qry = """ select distinct t1.bio_file_content_id, t2.bio_experiment_id, t1.file_type, t2.name
							 from bio_content t1, gse_analysis t2
							 where t1.file_type='Experiment Web Link' 
									and t1.location='geo/query/acc.cgi?acc='||t2.name
									and t1.repository_id=?
						"""
			biomart.eachRow(qry, [bioContentRepositoryId]) {
				bcrf.insertBioContentReference((long) it.bio_file_content_id, (long) it.bio_experiment_id, it.file_type, it.name)
			}
		}
	}



	void loadBioDataOmicMarker(Properties props, Sql biomart){

		if(props.get("skip_bio_data_omic_marker").toString().toLowerCase().equals("yes")){
			log.info "Skip loading data into BIO_DATA_OMIC_MARKER ..."
		}else{
			BioDataOmicMarker bdom = new BioDataOmicMarker()
			bdom.setBiomart(biomart)
			bdom.loadBioDataOmicMarker("GSE_ANALYSIS")
		}
	}


	void loadBioDataDisease(Properties props, Sql biomart){

		if(props.get("skip_bio_data_disease").toString().toLowerCase().equals("yes")){
			log.info "Skip loading BIO_DATA_DISEASE data ..."
		}else{

			BioDataDisease bdd = new BioDataDisease()
			bdd.setBiomart(biomart)

			Map diseaseMap = [:]
			String qry = "select bio_disease_id, disease from bio_disease"
			biomart.eachRow(qry) {
				diseaseMap[it.disease] = (long) it.bio_disease_id
			}

			log.info "Start loading BIO_DATA_DISEASE data ..."

			String projectInfoTable = props.get("project_info_table")

			qry = """ select bio_experiment_id, project_category
					  from $projectInfoTable t1, bio_experiment t2
				      where t1.project_accession=t2.accession """

			biomart.eachRow(qry){
				java.sql.Clob category = (java.sql.Clob) it.project_category
				if(!category.equals(null)){
					String [] str =  category.getCharacterStream().getText().split(";")
					for(int i in 0..str.size()-1){
						println i + "\t" + str[i].trim()
						if(str[i].indexOf("Diseases\\") != -1){
							String [] s = str[i].split("\\\\")
							for (int j in 0..s.size()-1){
								if(!diseaseMap[s[j].trim()].equals(null)){
									//println "\t" + j + "\t" + it.bio_experiment_id + "\t" + s[j].trim() + "\t" + diseaseMap[s[j].trim()]
									bdd.loadBioDataDisease((long) it.bio_experiment_id, diseaseMap[s[j].trim()], "OMICSOFT")
								}
							}
						}
					}
				}
			}

			log.info "End loading BIO_DATA_DISEASE data ..."
		}
	}



	void loadBioDataCompound(Properties props, Sql biomart){

		if(props.get("skip_bio_data_compound").toString().toLowerCase().equals("yes")){
			log.info "Skip loading BIO_DATA_COMPOUND data ..."
		}else{

			log.info "Start loading BIO_DATA_COMPOUND data ..."

			BioDataCompound bdc = new BioDataCompound()
			bdc.setBiomart(biomart)

			Map compoundMap = [:]
			String qry = """ select bio_compound_id, lower(generic_name) compound from bio_compound
							 union
							 select bio_compound_id, lower(code_name) compound from bio_compound
							 union
							 select bio_compound_id, lower(brand_name) compound from bio_compound
							 union
							 select bio_compound_id, lower(chemical_name) compound from bio_compound
							 union
							 select bio_compound_id, lower(mechanism) compound from bio_compound
							 union
							 select bio_compound_id, lower(description) compound from bio_compound """

			biomart.eachRow(qry) {
				compoundMap[it.compound] = (long) it.bio_compound_id
			}

			String projectInfoTable = props.get("project_info_table")

			qry = """ select bio_experiment_id, project_category
						  from $projectInfoTable t1, bio_experiment t2
						  where t1.project_accession=t2.accession """

			biomart.eachRow(qry){
				java.sql.Clob category = (java.sql.Clob) it.project_category
				if(!category.equals(null)){
					String [] str =  category.getCharacterStream().getText().split(";")
					for(int i in 0..str.size()-1){
						println i + "\t" + str[i].trim()
						if(str[i].indexOf("Chemicals and Drugs\\") != -1){
							String [] s = str[i].split("\\\\")
							for (int j in 0..s.size()-1){
								if(!compoundMap[s[j].trim().toLowerCase()].equals(null)){
									println "\t" + j + "\t" + it.bio_experiment_id + "\t" + s[j].trim() + "\t" + compoundMap[s[j].trim().toLowerCase()]
									bdc.loadBioDataCompound((long) it.bio_experiment_id, compoundMap[s[j].trim().toLowerCase()], "OMICSOFT")
								}
							}
						}
					}
				}
			}

			log.info "End loading BIO_DATA_COMPOUND data ..."
		}
	}


	void createGseAnalysisTable(Properties props, Sql biomart){

		String testsDataTable = props.get("tests_data_table")

		String qry = "select count(*) from user_tables where table_name='GSE_ANALYSIS'"

		if(biomart.firstRow(qry)[0] > 0){
			log.info "Start dropping the temporary table GSE_ANALYSIS ..."
			qry = "drop table GSE_ANALYSIS purge"
			biomart.execute(qry)
			log.info "The temporary table GSE_ANALYSIS is dropped..."
		}

		log.info "Start creating a temporary table GSE_ANALYSIS ..."

		qry = """ create table GSE_ANALYSIS nologging as
					   select name, platform, test, bio_experiment_id, bio_assay_analysis_id,
							 bio_assay_platform_id,
							 count(*) data_cnt, round(avg(foldchange),4) fc_mean,
							 round(stddev(foldchange),4) fc_stddev
					  from $testsDataTable t1, bio_experiment t2, bio_assay_analysis t3, bio_assay_platform t4
					  where t2.accession=t1.name and t3.analysis_name=t1.test and t3.etl_id=t1.name
							and t4.platform_name=t1.platform
					   group by name, platform, test, bio_experiment_id, bio_assay_analysis_id, bio_assay_platform_id
				  """

		biomart.execute(qry)

		log.info "The temporary table GSE_ANALYSIS is created ..."
	}


	void createAssayAnalysisDataTable(Properties props, Sql biomart){

		String testsDataTable = props.get("tests_data_table")

		String qry = "select count(*) from user_tables where table_name='ASSAY_ANALYSIS_DATA'"

		if(biomart.firstRow(qry)[0] > 0){
			log.info "Start dropping the temporary table ASSAY_ANALYSIS_DATA ..."
			qry = "drop table ASSAY_ANALYSIS_DATA purge"
			biomart.execute(qry)
			log.info "The temporary table ASSAY_ANALYSIS_DATA is dropped..."
		}

		log.info "Start creating a temporary table ASSAY_ANALYSIS_DATA ..."

		qry = """ create table ASSAY_ANALYSIS_DATA (
						 BIO_ASY_ANALYSIS_DATA_ID,
						 BIO_EXPERIMENT_ID,
						 BIO_ASSAY_PLATFORM_ID,
						 BIO_ASSAY_ANALYSIS_ID,
						 bio_assay_feature_group_id,
						 FEATURE_GROUP_NAME,
						 FOLD_CHANGE_RATIO,
						 RAW_PVALUE,
						 ADJUSTED_PVALUE,
						 PREFERRED_PVALUE,
						 TEA_NORMALIZED_PVALUE
					   ) nologging as
					   select
						  seq_bio_data_id.nextval,
						  t2.bio_experiment_id,
						  t2.bio_assay_platform_id,
						  t2.bio_assay_analysis_id,
						  t4.bio_assay_feature_group_id,
						  t1.probeset,
						  t1.foldchange,
						  t1.rawpvalue,
						  t1.adjustedpvalue,
						  t1.rawpvalue,
						  round(TEA_NPV_PRECOMPUTE(t1.foldchange, t2.fc_mean, t2.fc_stddev),4)
						from $testsDataTable t1, gse_analysis t2, bio_assay_feature_group t4
						where t1.name=t2.name and t1.platform=t2.platform and t1.test=t2.test and
							 to_char(t4.feature_group_name)=t1.probeset and t2.fc_stddev >0
				  """
		biomart.execute(qry)

		log.info "The temporary table ASSAY_ANALYSIS_DATA is created ..."
	}



}
