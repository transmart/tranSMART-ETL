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
  

package com.recomdata.pipeline.transmart


import java.io.File;

import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger

class Pathway {

	private static final Logger log = Logger.getLogger(Pathway)

	Sql deapp
	String source

	void loadPathway(String pathwayTable, String pathwayDataTable){

		log.info "Start loading ${source} pathway data into DE_PATHWAY ..."

		String qry = """ insert into de_pathway(name, description, source, externalid, pathway_uid)
						 select t1.descr, t1.descr, ? , t1.pathway, 'PATHWAY:' || t1.pathway 
						 from ${pathwayTable} t1, ${pathwayDataTable} t2
						 where t1.pathway=t2.pathway
						 minus
						 select to_char(name), to_char(description), to_char(source), to_char(externalid), to_char(pathway_uid)
						 from de_pathway where source=?
					 """
		deapp.execute(qry, [source, source])

		log.info "End loading ${source} pathway data into DE_PATHWAY ..."
	}



	void loadPathway(File pathwayDefinition){

		String qry = "insert into de_pathway(name, description, source, externalid, pathway_uid) values(?, ?, ?, ?, ?)"

		if(pathwayDefinition.exists()){

			if(isPathwaySourceExist()){
				log.warn("Pathway from $source already loaded into de_pathway and it'll be delete before loading ...")

				log.info("Start deleteing data for ${source} in DE_PATHWAY and DE_PATHWAY_GENE ...")

				String qry1 = """ delete from de_pathway_gene 
									where pathway_id in (select id from de_pathway where source=?) """
				deapp.execute(qry1, [source])

				qry1 = """ delete from de_pathway where source=? """
				deapp.execute(qry1, [source])
			}

			log.info("Start loading " + pathwayDefinition.toString() + " into de_pathway")
			deapp.withTransaction {
				deapp.withBatch(qry, { ps ->
					pathwayDefinition.eachLine{
						String [] str = it.split("\t")
						ps.addBatch([
							str[1],
							str[1],
							source,
							str[0],
							"PATHWAY:" + str[0]
						])
					}
				})
			}
		}else{
			throw new RuntimeException("Cannot find file: " + pathwayDefinition.toString())
		}
	}


	void loadPathwayDefinition(File pathwayDef){

		String [] str = []
		Map rec = [:]
		if(pathwayDef.exists()){
			pathwayDef.eachLine{
				str = it.split("\t")
				insertPathway(str[0], str[1])
			}
		}else{
			log.error("Cannot find Pathway Definition file: " + pathwayDef.toString())
			throw new RuntimeException("Cannot find Pathway Definition file: " + pathwayDef.toString())
		}
	}



	boolean isPathwaySourceExist(){
		String qry = "select count(1) from de_pathway where source =?"
		if(deapp.firstRow(qry, [source])[0] > 0) return true
		else return false
	}


	boolean isPathwayExist(String pathwayId){
		String qry = "select count(1) from de_pathway where pathway_uid =?"
		if(deapp.firstRow(qry, [
			"PATHWAY:" + source + ":" + pathwayId
			])[0] > 0) return true
		else return false
	}


	void insertPathway(String pathwayId, String pathwayName){
		String qry = "insert into de_pathway(name, description, source, externalid, pathway_uid) values(?,?,?,?,?)"
		if(isPathwayExist(pathwayId)){
			log.info "Pathway \"$pathwayId - $pathwayName\" aleardy exitst ..."
		} else {
			log.info "Loading the pathway \"$pathwayId - $pathwayName\" ..."
			deapp.execute(qry, [
				pathwayName,
				pathwayName,
				source,
				pathwayId,
				"PATHWAY:" + source + ":" + pathwayId
			])
		}
	}


	Map getPathwayId(){
		Map pathway = [:]
		String qry = "select id, externalid from de_pathway where source=?"
		deapp.eachRow(qry,[source]) {
			pathway[it.externalId] = it.id
		}
	}


	void setSource(String source){
		this.source = source
	}


	void setDeapp(Sql deapp){
		this.deapp = deapp
	}
}
