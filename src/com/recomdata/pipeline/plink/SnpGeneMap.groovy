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

import java.io.File;

import org.apache.log4j.Logger;

import groovy.sql.Sql

class SnpGeneMap {

	private static final Logger log = Logger.getLogger(SnpGeneMap)

	Sql deapp
	String annotationTable

	void loadSnpGeneMap(File snpGeneMap){

		createTempSnpGeneMapTable("tmp_de_snp_gene_map")

		// store unique set of SNP ID -> Gene ID
		Map rs = [:]
		if(snpGeneMap.size() >0){
			log.info("Start loading " + snpGeneMap.toString() + " into DE_SNP_GENE_MAP ...")
			snpGeneMap.eachLine{
				String [] str = it.split(/\t/)
				rs[str[0]] = str[1]
			}
		}else{
			log.error(snpGeneMap.toString() + " is empty.")
		}


		String qry = """ insert into tmp_de_snp_gene_map(entrez_gene_id, snp_id, snp_name)
							 select ?, snp_info_id, ?
							 from de_snp_info where name=? """
		if(snpGeneMap.size() > 0){
			deapp.withTransaction {
				deapp.withBatch(qry, {stmt ->
					rs.each{k, v ->
						stmt.addBatch([v, k, k])
					}
				})
			}
		}

		loadSnpGeneMap("tmp_de_snp_gene_map")

		log.info "Drop the temp table TMP_DE_SNP_GENE_MAP "
		qry = " drop table tmp_de_snp_gene_map purge"
		deapp.execute(qry)
	}

	
	void loadSnpGeneMap(String tmpSnpGeneMapTable){

		log.info "Start loading data into the table DE_SNP_GENE_MAP ... "

		String qry = """insert into de_snp_gene_map nologging (snp_id, snp_name, entrez_gene_id)
						select t2.snp_info_id, t1.snp_name, t1.entrez_gene_id
						from $tmpSnpGeneMapTable t1, de_snp_info t2
						where t1.snp_name = t2.name 
						minus
						select snp_id, snp_name, entrez_gene_id from de_snp_gene_map"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_GENE_MAP ... "
	}


	void loadSnpGeneMap(Map columnMap){

		log.info "Start loading data into the table DE_SNP_GENE_MAP ... "

		String qry = """insert into de_snp_gene_map nologging (snp_id, snp_name, entrez_gene_id)
						select t2.snp_info_id, t1.${columnMap["probe"]}, t1.${columnMap["gene_id"]}
						from $annotationTable t1, de_snp_info t2
						where t1.${columnMap["probe"]} = t2.name and t1.${columnMap["gene_id"]} not like '---%'
                        minus
						select snp_id, snp_name, to_char(entrez_gene_id) from de_snp_gene_map"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_GENE_MAP ... "
	}


	void createTempSnpGeneMapTable(String tempSnpGeneMapTable){

		String qry = "select count(1) from user_tables where table_name=upper(?)"
		if(deapp.firstRow(qry, [tempSnpGeneMapTable])[0] > 0){
			log.info "Drop table $tempSnpGeneMapTable ..."
			qry = "drop table $tempSnpGeneMapTable purge"
			deapp.execute(qry)
		}

		log.info "Start creating the temp table $tempSnpGeneMapTable ..."

		qry = """ create table tmp_de_snp_gene_map as select * from de_snp_gene_map where 1=2 """
		deapp.execute(qry)

		log.info "End creating table $tempSnpGeneMapTable ..."
	}


	void setAnnotationTable(String annotationTable){
		this.annotationTable = annotationTable
	}


	void setDeapp(Sql deapp){
		this.deapp = deapp
	}

}
