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

import org.apache.log4j.Logger;

import groovy.sql.Sql

class SnpInfo {

	private static final Logger log = Logger.getLogger(SnpInfo)

	Sql deapp
	String annotationTable

	void loadSnpInfo(File snpMap){

		String qry

		createTempSnpInfoTable("TMP_DE_SNP_INFO")

		qry = "insert into tmp_de_snp_info(name, chrom, chrom_pos) values(?, ?, ?)"
		if(snpMap.size() > 0){
			deapp.withTransaction {

				log.info("Start loading " + snpMap.toString() + " into TMP_DE_SNP_INFO ...")

				deapp.withBatch(qry, {stmt ->
					snpMap.eachLine{
						String [] str = it.split(/\t/)
						stmt.addBatch([str[1], str[0], str[3]])
					}
				})
			}
		}else{
			log.error(snpMap.toString() + " is empty.")
		}


		loadSnpInfo("tmp_de_snp_info")

		log.info "Drop the temp table TMP_DE_SNP_INFO "
		qry = " drop table tmp_de_snp_info purge"
		deapp.execute(qry)
	}


	void loadSnpInfo(String tmpSnpInfoTable){

		log.info "Start loading data into the table DE_SNP_INFO ... "

		String qry = """insert into de_snp_info nologging (name, chrom, chrom_pos)
						select distinct name, chrom, chrom_pos
						from $tmpSnpInfoTable
						where upper(name) not in (select upper(name) from de_snp_info)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_INFO ... "
	}


	void loadSnpInfo(){

		log.info "Start loading data into the table DE_SNP_INFO ... "

		String qry = """insert into de_snp_info nologging (name, chrom, chrom_pos)
					    select distinct snp_id, chrom, pos
						from """ + annotationTable + """
                        where upper(snp_id) not in (select upper(name) from de_snp_info)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_INFO ... "
	}


	void loadSnpInfo(Map columnMap){

		log.info "Start loading data into the table DE_SNP_INFO ... "

		String qry = """insert into de_snp_info nologging (name, chrom, chrom_pos)
						select distinct ${columnMap["name"]}, ${columnMap["chr"]}, ${columnMap["pos"]}
						from  $annotationTable 
						where upper(${columnMap["name"]}) not in (select upper(name) from de_snp_info)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_INFO ... "
	}

	
	void createTempSnpInfoTable(String tempSnpInfoTable){

		String qry = "select count(1) from user_tables where table_name=upper(?)"
		if(deapp.firstRow(qry, [tempSnpInfoTable])[0] > 0){
			log.info "Drop table $tempSnpInfoTable ..."
			qry = "drop table $tempSnpInfoTable purge"
			deapp.execute(qry)
		}

		log.info "Start creating the temp table $tempSnpInfoTable ..."

		qry = """ create table tmp_de_snp_info as select * from de_snp_info where 1=2 """
		deapp.execute(qry)

		log.info "End creating table $tempSnpInfoTable ..."
	}


	void setAnnotationTable(String annotationTable){
		this.annotationTable = annotationTable
	}

	void setSql(Sql deapp){
		this.deapp = deapp
	}
}
