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

class SnpProbe {

	private static final Logger log = Logger.getLogger(SnpProbe)

	Sql deapp
	String annotationTable

	void loadSnpProbe(File probeInfo){

		// create a temp table for snp_probe data
		createTempSnpProbeTable("tmp_de_snp_probe")

		// store unique set of SNP ID -> rs#
		Map rs = [:]
		if(probeInfo.size() >0){
			log.info("Start loading " + probeInfo.toString() + " into DE_SNP_PROBE ...")
			probeInfo.eachLine{
				String [] str = it.split(/\t/)
				rs[str[0]] = str[1]
			}
		}else{
			log.error(probeInfo.toString() + " is empty.")
		}

		String qry = """ insert into tmp_de_snp_probe(probe_name, snp_name) values(?, ?) """
		if(probeInfo.size() > 0){
			deapp.withTransaction {
				deapp.withBatch(qry, {stmt ->
					rs.each{k, v ->
						stmt.addBatch([k, v])
					}
				})
			}
		}

		loadSnpProbe("tmp_de_snp_probe")

		log.info "Drop the temp table TMP_DE_SNP_PROBE "
		qry = " drop table tmp_de_snp_probe purge"
		deapp.execute(qry)
	}

	
	void loadSnpProbe(String tmpSnpProbeTable){

		log.info "Start loading data into the table DE_SNP_PROBE ... "
		
		String qry = """insert into de_snp_probe nologging (probe_name, snp_id, snp_name)
						select t1.probe_name, t2.snp_info_id, t1.snp_name
						from $tmpSnpProbeTable t1, de_snp_info t2
						where t1.probe_name=t2.name and snp_info_id not in (select snp_id from de_snp_probe)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_PRObE ... "
	}
	
	
	void loadSnpProbe(){

		log.info "Start loading data into the table DE_SNP_PROBE ... "

		String qry = """insert into de_snp_probe nologging (probe_name, snp_id, snp_name)
						select t1.snp_id, t2.snp_info_id, t1.rs_id
						from """ + annotationTable + """ t1, de_snp_info t2
						where upper(t1.snp_id)=upper(t2.name) and t1.rs_id is not null and
							  upper(snp_id) not in (select upper(snp_name) from de_snp_probe)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_PRObE ... "
	}

	

	void loadSnpProbe(Map columnMap){

		log.info "Start loading data into the table DE_SNP_PROBE ... "

		String qry = """insert into de_snp_probe nologging (probe_name, snp_id, snp_name)
						select distinct t1.${columnMap["probe"]}, t2.snp_info_id, t1.${columnMap["rs"]}
						from """ + annotationTable + """ t1, de_snp_info t2
						where upper(t1.${columnMap["probe"]})=upper(t2.name) and t1.${columnMap["rs"]} is not null 
							  and t2.snp_info_id not in (select snp_id from de_snp_probe)"""
		deapp.execute(qry)

		log.info "End loading data into the table DE_SNP_PRObE ... "
	}


	void createTempSnpProbeTable(String tempSnpProbeTable){

		String qry = "select count(1) from user_tables where table_name=upper(?)"
		if(deapp.firstRow(qry, [tempSnpProbeTable])[0] > 0){
			log.info "Drop table $tempSnpProbeTable ..."
			qry = "drop table $tempSnpProbeTable purge"
			deapp.execute(qry)
		}

		log.info "Start creating the temp table $tempSnpProbeTable ..."

		qry = """ create table tmp_de_snp_probe as select * from de_snp_probe where 1=2 """
		deapp.execute(qry)

		log.info "End creating table $tempSnpProbeTable ..."
	}


	void setAnnotationTable(String annotationTable){
		this.annotationTable = annotationTable
	}

	void setSql(Sql deapp){
		this.deapp = deapp
	}
}
