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
  

package com.recomdata.pipeline.annotation

import java.io.File;
import java.util.Properties;

import com.recomdata.pipeline.util.Util
import groovy.sql.Sql
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator


class AgilentGPL {

	private static final Logger log = Logger.getLogger(AgilentGPL)

	Sql biomart, deapp
	String annotationTable, organism, platform

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()

		Properties props = Util.loadConfiguration("conf/loader.properties")

		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		AgilentGPL agilent = new AgilentGPL()
		agilent.setDeapp(deapp)
		agilent.setAnnotationTable(props.get("agilent_annotation_table").toString().trim())
		agilent.setOrganism("Home sapiens")
		agilent.loadAgilentGPL(props, deapp)
		agilent.loadSnpInfo(deapp)
		agilent.loadSnpProbe(deapp)
		agilent.loadSnpGeneMap(deapp)
		agilent.loadGplInfo(deapp)
	}


	void loadAgilentGPL(Properties props, Sql deapp){

		if(props.get("skip_agilent_annotation_loader").toString().trim().toLowerCase().equals("yes")){
			log.info "Skip loading Agilent annotation file(s) ..."
		}else{

			log.info "Start loading Agilent annotation file(s) from ${props.get("agilent_annotation_source")} ..."

			if(props.get("recreate_agilent_annotation_table").toString().trim().toLowerCase().equals("yes")){
				log.info "Start recreating Agilent annotation table ${props.get("agilent_annotation_table")} ..."
				createAnnotationTable()
			}

			String annotationSourceDirectory = props.get("agilent_annotation_source").toString().trim()
			String agilentList = props.get("agilent_list").toString().trim()

			if(agilentList.indexOf(",") != -1){
				String [] gplList = agilentList.split(/\,/)
				gplList.each {
					File annotationSource = new File(annotationSourceDirectory + File.separator + it)
					readAgilentGPL(annotationSource)
				}
			}else{
				File annotationSource = new File(annotationSourceDirectory + File.separator + agilentList)
				readAgilentGPL(annotationSource)
			}

			log.info "End loading Agilent annotation file(s) from ${props.get("agilent_annotation_source")} ..."
		}
	}



	// not in use
	void loadAgilentGPLs(File input){

		if(input.isDirectory()){
			input.eachFile {
				if((it.toString().indexOf("GPL.GPL") != -1) && (it.toString().indexOf(".txt") != -1)){
					log.info "Start loading GPL GX annotation file ${it.toString()} ..."
					readAgilentGPL(it)
				}
			}
		} else{
			if(input.isFile()) {
				log.info "Start loading GPL GX annotation file: ${input.toString()} ..."
				readAgilentGPL(input)
			} else {
				log.error "Neither a directory nor a file: " + input.toString()
			}
		}
	}


	/**
	 #0 ID = Agilent feature number
	 #1 CONTROL_TYPE = Control type
	 #2 GB_ACC = GenBankAccession
	 #3 GENE_SYMBOL = Gene Symbol
	 #4 GENE_NAME = Gene Name
	 #5 ACCESSION_STRING = Accession String
	 #6 CHROMOSOMAL_LOCATION = Chromosomal Location
	 #7 CYTOBAND = Cytoband
	 #8 DESCRIPTION = Description
	 #9 GB_RANGE = NCBI Build 35.1 Accession.Version[start..end]
	 * @param annotation
	 */

	void readAgilentGPL(File annotation){

		// extract platform info from filename, and assuming filename follows:
		//    <platform>-<version>.txt or <platform>.txt
		String inputName = annotation.getName()
		if(inputName.indexOf("-") != -1) platform = inputName.split("-")[0]
		else platform =  inputName.split(".")[0]


		String out = annotation.toString().replace("txt", "tsv")

		StringBuffer sb = new StringBuffer()

		if(annotation.size() > 0){
			log.info "Start reading ${annotation.toString()} ..."

			String [] str
			String chr, startLocation, stopLocation

			annotation.each{
				if((it.indexOf("#") !=0) && (it.indexOf("ID") !=0) && (it.indexOf("\t") != -1)){
					str = it.split ("\t")
					if(!str[6].equals(null) && (str[6].indexOf(":") != -1)){
						chr = str[6].split (":")[0]
						startLocation = str[6].split (":")[1].split("-")[0].trim()
						stopLocation = str[6].split (":")[1].split("-")[1].trim()

						sb.append(platform + "\t")
						sb.append(organism + "\t")
						sb.append(str[0] + "\t")
						sb.append(str[3] + "\t")
						sb.append(str[4] + "\t")
						sb.append(chr + "\t")
						sb.append(startLocation + "\t")
						sb.append(stopLocation + "\n")
					}
				}
			}
			log.info "End reading ${annotation.toString()} ..."
		}else{
			log.error("Empty file: ${annotation.toString()} ... ")
		}

		File output = new File(out)
		if(output.size() >0){
			output.delete()
			output.createNewFile()
		}
		output.append(sb.toString())

		loadAgilentGPL(output)
	}


	void loadAgilentGPL(File input){
		if(input.size() >0){
			log.info "Start loading Agilent annotation data from ${input.toString()} ..."
			deapp.withTransaction {
				deapp.withBatch(""" insert into $annotationTable (platform, species, probe_id, gene_symbol, gene_descr, gene_id,
											chromosome, start_loc, stop_loc) values (?, ?, ?, ?, ?,  ?, ?, ?, ?) """, { ps ->
							input.eachLine{
								String [] str = it.split(/\t/)

								if(!str.equals(null) && str.size() > 7){
									ps.addBatch([
										str[0],
										str[1],
										str[2],
										str[3],
										str[4],
										'',
										str[5],
										str[6],
										str[7]
									])
								} else{
									println it
								}
							}
						})
			}

			log.info "Stop loading Agilent annotation data from ${input.toString()} ..."
		}else{
			log.error("Empty file: " + input.toString())
		}
	}


	void loadSnpInfo(Sql deapp){
		log.info "Start loading into DE_SNP_INFO ..."
		
		String qry = """ insert into de_snp_info (name, chrom, chrom_pos)
						 select probe_id, replace(substr(chromosome,4), '_random', ''), start_loc
						 from $annotationTable 
						 where probe_id not in (select name from de_snp_info)"""
		deapp.execute(qry)
		
		log.info "End loading into DE_SNP_INFO ..."
	}
	
	
	void loadSnpProbe(Sql deapp){
		log.info "Start loading into DE_SNP_PROBE ..."
		
		String qry = """ insert into de_snp_probe (probe_name, snp_id, snp_name)
						 select name, snp_info_id, name
						 from de_snp_info t1,  $annotationTable t2
						 where t1.name=t2.probe_id and t1.snp_info_id not in (select snp_id from de_snp_probe)"""
		deapp.execute(qry)
		
		log.info "End loading into DE_SNP_PROBE ..."
	}
	
	
	void loadSnpGeneMap(Sql deapp){
		log.info "Start loading into DE_SNP_GENE_MAP ..."
		
		String qry = """ insert into de_snp_gene_map (snp_id, snp_name, entrez_gene_id)
						 select distinct t3.snp_info_id, t2.bio_marker_name, t2.primary_external_id 
						 from $annotationTable  t1, biomart.bio_marker t2, de_snp_info t3
						 where t1.gene_symbol is not null and t1.gene_symbol=t2.bio_marker_name 
							   and upper(t2.organism)='HOMO SAPIENS' 
								and t1.probe_id = t3.name and t3.snp_info_id not in (select snp_id from de_snp_gene_map) """
		deapp.execute(qry)
		
		log.info "End loading into DE_SNP_GENE_MAP ..."
	}
	
	
	void loadGplInfo(Sql deapp){
		log.info "Start loading into DE_GPL_INFO ..."
		
		String qry = """ insert into de_gpl_info (platform, title, organism, marker_type, annotation_date) values(?, ?, ?, ?, sysdate) 
							   """
		deapp.execute(qry,[platform, 'Agilent-014693 Human Genome CGH Microarray 244A (Probe name version)', 'Homo sapiens', 'SNP'])
		
		log.info "End loading into DE_GPL_INFO ..."
	}
	
	
	boolean isAgilentGPLAnnotationExist(String platform){
		String qry = "select count(*) from $annotationTable where upper(platform)=?"
		if(deapp.firstRow(qry, [platform.toUpperCase()])[0] > 0){
			return true
		} else {
			return false
		}
	}


	void createAnnotationTable(){
		String qry = "select count(*) from user_tables where table_name=?"
		if(deapp.firstRow(qry, [
			annotationTable.toUpperCase()
		])[0] > 0){
			log.info "The existing table $annotationTable will be dropped ..."
			qry = "drop table $annotationTable purge"
			deapp.execute(qry)
		}

		log.info "Start creating table $annotationTable ..."
		qry = """ create table $annotationTable(
						platform 	varchar2(100),
						species  	varchar2(100),
						probe_id 	varchar2(100),
						gene_symbol varchar2(100),
						gene_descr  varchar2(1000),
						gene_id    	varchar2(20),
						chromosome  varchar2(20),
                        start_loc	number(10),
						stop_loc		number(10)
					) """
		deapp.execute(qry)
		log.info "End creating table $annotationTable ..."
	}



	void setAnnotationTable(String annotationTable){
		this.annotationTable = annotationTable
	}

	
	void setOrganism(String organism){
		this.organism = organism
	}

	
	void setDeapp(Sql deapp){
		this.deapp = deapp
	}

	
	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
