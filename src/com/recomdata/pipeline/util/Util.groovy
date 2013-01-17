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
  

package com.recomdata.pipeline.util

import groovy.sql.Sql
import java.io.IOException;
import java.sql.SQLClientInfoException;
import java.util.Properties;

import org.apache.log4j.Logger;

class Util {

	private static final long MEGABYTE = 1024L * 1024L;

	private static final Logger log = Logger.getLogger(Util)

	/**
	 * load and combine common.properties with data specific property file
	 *  
	 * @param file		data type specific property file
	 * @return
	 * @throws IOException
	 */
	static Properties loadConfiguration(String file) throws IOException {

		Properties prop1 = new Properties();
		FileInputStream fis = new FileInputStream("conf/Common.properties");
		prop1.load(fis);
		fis.close();
		def common = new ConfigSlurper().parse(prop1)

		def config
		if(file.equals("")){
			config = common
		}else{
			Properties prop2 = new Properties();
			FileInputStream fism = new FileInputStream(file);
			prop2.load(fism);
			fism.close();
			def module = new ConfigSlurper().parse(prop2)

			config = common.merge(module)
		}

		return config.toProperties()
	}


	/**
	 *
	 * @param s		mapping between sample type to concept code
	 * @return
	 */
	static Map convertStringToMap(String s){

		Map map = [:]
		if(s.indexOf(";") != -1){
			String [] str = s.split(";")
			for(i in 0..str.size()-1){
				if(str[i].indexOf(":") != -1) {
					String [] str1 = str[i].split(":")
					if(!str1[0].equals(null))
						map[str1[0].trim()] = str1[1].trim()
				}
			}
		}else{
			if(s.indexOf(":") != -1){
				String [] temp = s.split(":")
				if(!temp[0].equals(null))
					map[temp[0].trim()] = temp[1].trim()
			}
		}
		return map
	}


	static void createAnnotationTable(Sql sql){
		String qry = "select count(1) from user_tables where table_name='ANNOTATION'"

		if(sql.firstRow(qry)[0] > 0){
			sql.execute "drop table annotation purge"
		}

		sql.execute """ create table annotation (
					 		snp_id 	varchar2(200),
					 		rs_id	varchar2(200),
							chrom 	varchar2(10),
					 		pos		number(10)
			        	)"""
	}


	static void createSNPGeneMappingTable(Sql sql){
		String qry = "select count(1) from user_tables where table_name='SNP_GENE_MAP'"
		if(sql.firstRow(qry)[0] > 0){
			sql.execute "drop table snp_gene_map purge"
		}

		sql.execute """ create table snp_gene_map (
							 snp_id 	varchar2(100),
							 rs_id   	varchar2(100),
							 organism	varchar2(100),
							 gene_id    varchar2(20),
							 gene_name  varchar2(200),
							 gene_desc 	varchar2(500)
						)"""
	}


	static void loadSNPInfo(Sql sql){
		String qry = """insert into de_snp_info(name, chrom, chrom_pos)
						 select rs """
	}


	static void loadProbeInfo(Sql sql){
		String qry = """ """
	}

	static void loadSNPGeneMap(Sql sql){
		String qry = """ """
	}

	/**
	 *   used for testing purpose
	 *   
	 * @return
	 */
	static createLocalDeapp(){
		String driver = "oracle.jdbc.driver.OracleDriver"
		String url = "jdbc:oracle:thin:@localhost:1521:orcl"
		return Sql.newInstance(url, "deapp", "deapp", driver)
	}



	static createRDCVMDeapp(){
		String driver = "oracle.jdbc.driver.OracleDriver"
		String url = "jdbc:oracle:thin:@192.168.41.17:1521:orcl"
		return Sql.newInstance(url, "deapp", "deapp", driver)
	}


	/**
	 * Parse Properties object and then create/return Sql object 
	 * for later use
	 * 
	 * @param prop		Properties object
	 * @return			groovy.Sql object
	 */

	static Sql createSqlFromPropertyFile(Properties prop){

		String driver = prop.get("driver_class")
		String url = prop.get("url")
		String username = prop.get("username")
		String password = prop.get("password")
		return Sql.newInstance(url, username, password, driver)
	}


	/**
	 * create a groovy.Sql object for a specific schema
	 *  
	 * @param prop		Properties object
	 * @param schema	schema name
	 * @return			groovy.Sql object
	 */
	static Sql createSqlFromPropertyFile(Properties prop, String schema){

		String driver = prop.get("driver_class")
		String url = prop.get("url")
		String username = prop.get(schema.toLowerCase() + "_username")
		String password = prop.get(schema.toLowerCase() + "_password")
		return Sql.newInstance(url, username, password, driver)
	}

	/*
	 * switch Chromosome naming:  "X" <-> 23, "Y" <-> 24, "XY" <-> 25 and "MT" <-> 26  
	 * 
	 * @param deapp
	 * @param tableName
	 * @param columnName
	 * @param oldChrName
	 * @param newChrName
	 * @return
	 */
	static void normalizeChromosomeNaming(Sql deapp, String tableName, String columnName, String oldChrName, String newChrName){
		String update = "update " + tableName + " set " + columnName + "=? where upper(" + columnName + ")=?"
		deapp.execute(update, [newChrName, oldChrName])
	}


	/**
	 * 
	 * @param deapp
	 * @param tableName
	 * @param columnName
	 * @param nameMapping		Mapping oldName -> newName
	 * @return
	 */
	static void normalizeChromosomeNaming(Sql deapp, String tableName, String columnName, List nameMapping){
		nameMapping.each{
			String [] str = it.split(":")
			normalizeChromosomeNaming(deapp, tableName, columnName, str[1], str[0])
		}
	}


	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}


	static String getMemoryUsage(Runtime runtime){
		// Run the garbage collector
		runtime.gc();

		// Calculate the used memory
		long memory = runtime.totalMemory() - runtime.freeMemory();

		return "JVM Memory Usage: Total: ${bytesToMegabytes(runtime.totalMemory())}MB; Free: ${bytesToMegabytes(runtime.freeMemory())}MB; Used: ${bytesToMegabytes(memory)}MB"
	}


	static List getChromosomeList(){
		List chrs = []
		for(i in 1..24) chrs[i-1] = i.toString()
		return chrs
	}


	static List getChromosomeListWitLetter(){
		List chrs = []
		for(i in 1..22) chrs[i-1] = i.toString()
		chrs[22] = "X"
		chrs[23] = "Y"
		//chrs[25] = "XY"
		//chrs[26] = "MT"
		return chrs
	}


	/**
	 *  truncate all tables used by SNP data
	 * 
	 * @param sql		should point to deapp schema
	 * @return
	 */
	static truncateSNPTable(Sql sql){

		String qry

		qry = "truncate table de_snp_info";
		sql.execute(qry)

		qry = "truncate table de_snp_probe";
		sql.execute(qry)

		qry = "truncate table de_snp_gene_map";
		sql.execute(qry)

		qry = "truncate table de_subject_snp_dataset";
		sql.execute(qry)

		qry = "truncate table de_snp_data_dataset_loc";
		sql.execute(qry)

		qry = "truncate table de_snp_probe_sorted_def";
		sql.execute(qry)

		qry = "truncate table de_snp_data_by_patient";
		sql.execute(qry)

		qry = "truncate table de_snp_data_by_probe";
		sql.execute(qry)

		qry = "truncate table de_snp_subject_sorted_def";
		sql.execute(qry)
	}

	static String cleanupId(String ind){
		return ind.replace("G", "").replace("T", "").replace("_2", "").replace("NA", "")
	}

	static void printMap(Map map){

		int index = 1
		map.each{ k, v ->
			println "$index: $k  ->  $v"
			index++
		}
	}


	static void createTestsTable(Sql sql, String tableName){

		String qry = """ create table $tableName (
							 NAME            VARCHAR2(100),
							 PLATFORM        VARCHAR2(100),
							 ID              NUMBER,
							 TEST            VARCHAR2(500),
							 PROBESET        VARCHAR2(100),
							 ESTIMATE        NUMBER(22,10),
							 FOLDCHANGE      NUMBER(22,10),
							 RAWPVALUE       NUMBER(22,10),
							 ADJUSTEDPVALUE  NUMBER(22,10),
							 MAXLSMEAN       NUMBER(22,10),
							 CONTRASTNAME    VARCHAR2(1000),
							 VARIABLENAME    VARCHAR2(1000)
					   )"""

		String qry1 = "select count(1) from user_tables where table_name=upper(?)"
		if(sql.firstRow(qry1, [tableName])[0] > 0) {
			log.info "Start dropping the table $tableName ... "
			qry1 = "drop table $tableName purge"
			sql.execute(qry1)
			log.info "Start dropping the table $tableName ... "
		}

		log.info "Start creating the table $tableName ... "
		sql.execute(qry)
		log.info "End creating the table $tableName ... "
	}


	static void truncateTestsTable(Sql sql, String tableName){

		log.info "Start truncating the table $tableName ... "

		String qry = "truncate table $tableName"
		sql.execute(qry)

		log.info "End truncating the table $tableName ... "
	}


	static void truncateProjectInfoTable(Sql sql, String projectInfoTable){

		log.info "Start truncating the table $projectInfoTable ... "

		String qry = "truncate table $projectInfoTable"
		sql.execute(qry)

		log.info "End truncating the table $projectInfoTable ... "
	}


	static void createProjectInfoTable(Sql sql, String projectInfoTable){

		String qry = "select count(1) from user_tables where table_name=?"

		// if the table exists, then drop it first
		if(sql.firstRow(qry, [
			projectInfoTable.toUpperCase()
		])[0] > 0){

			log.info "Start dropping the table $projectInfoTable ... "

			qry = "drop table $projectInfoTable purge"
			sql.execute(qry)

			log.info "End dropping the table $projectInfoTable ... "
		}

		log.info "Start creating the table $projectInfoTable ... "

		qry = """ CREATE TABLE $projectInfoTable
			  (
				-- FILE_ID NUMBER(5) PRIMARY KEY,
				-- ID VARCHAR2(100) UNIQUE,
				Name VARCHAR2(100) PRIMARY KEY,
				File_Name VARCHAR2(100) UNIQUE,
				ActiveDataName VARCHAR2(400),
				Project_Accession VARCHAR2(100),
				Project_Category CLOB, -- VARCHAR2(4000),
				Project_ContactAddress VARCHAR2(4000),
				Project_ContactCompany VARCHAR2(400),
				Project_ContactDepartment VARCHAR2(400),
				Project_ContactEmail VARCHAR2(400),
				Project_ContactLaboratory VARCHAR2(400),
				Project_ContactName VARCHAR2(400),
				Project_ContactPhone VARCHAR2(100),
				Project_ContactWebLink VARCHAR2(1000),
				Project_Contributors VARCHAR2(4000),
				Project_Description CLOB, -- VARCHAR2(4000),
				Project_Design CLOB, -- VARCHAR2(4000),
				Project_ID VARCHAR2(100),
				Project_Keywords VARCHAR2(1000),
				Project_Organism VARCHAR2(400),
				Project_Tissue VARCHAR2(400),
				Project_Compound VARCHAR2(400),
				Project_Platform VARCHAR2(400),
				Project_PlatformDescription VARCHAR2(4000),
				Project_PlatformOrganism VARCHAR2(400),
				Project_PlatformProvider VARCHAR2(400),
				Project_PlatformTechnology VARCHAR2(400),
				Project_PlatformType VARCHAR2(400),
				Project_PubMed VARCHAR2(400),
				Project_StudyType VARCHAR2(1000),
				Project_SupplementaryFile VARCHAR2(400),
				Project_Title VARCHAR2(500),
				Project_WebLink VARCHAR2(1000),
				PROJECT_EXTRACTEDFROMCELFILES VARCHAR2(1000),
				Project_ContactOrganization NVARCHAR2(1000),
				Project_ContactFax VARCHAR2(100),
				Project_OutputFile VARCHAR2(500),
				Project_DataSource NVARCHAR2(100),
				Project_Editors VARCHAR2(100),
				Project_IsPrivate VARCHAR2(100),
				Project_PublishDate VARCHAR2(100),
				entrydt date
			)
			NOLOGGING
		"""
		sql.execute(qry)

		log.info "End creating the table $projectInfoTable ... "
	}

}