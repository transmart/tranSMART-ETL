/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version, along with the following terms:
 *
 * 1.	You may convey a work based on this program in accordance with section 5,
 *      provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it,
 *      in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************/
  

package org.transmartproject.pipeline.transmart


import java.io.File;
import java.util.Map;

import org.transmartproject.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import groovy.sql.GroovyRowResult

class PathwayGene {

	private static final Logger log = Logger.getLogger(PathwayGene)

	Sql deapp
	Sql biomart
	Sql biomartuser
	String source

	void loadPathwayGene(String pathwayDataTable){

            String qry
            String qry1
            String qry2
            String qry3
            String qry4
            Boolean isPostgres = Util.isPostgres()
            if(isPostgres) {
    
		qry1 = """ select gene_symbol, organism, pathway
					 from ${pathwayDataTable} """
                qry2 = """ select id from de_pathway where externalid=?"""
		qry3 = """ select count(*) from de_pathway_gene where pathway_id=? and gene_symbol=? and gene_id=? """
		qry4 = """ insert into de_pathway_gene(pathway_id, gene_symbol, gene_id) values(?,?,?)"""
            }
            else
            {
		qry1 = """ select gene_symbol, organism, pathway
					 from ${pathwayDataTable} """
                qry2 = """ select id from de_pathway where externalid=?"""
		qry3 = """ select count(*) from de_pathway_gene where pathway_id=? and gene_symbol=? and gene_id=? """
		qry4 = """ insert into de_pathway_gene(pathway_id, gene_symbol, gene_id) values(?,?,?)"""
		qry = """ insert into de_pathway_gene(pathway_id, gene_symbol, gene_id)
						 select t1.id, t2.gene_symbol, t3.primary_external_id
						 from de_pathway t1, ${pathwayDataTable} t2, biomart.bio_marker t3
						 where t1.externalid=t2.pathway and t1.source=? 
							 and upper(t2.gene_symbol)=upper(t3.bio_marker_name) and t3.bio_marker_type='GENE' 
							 and upper(t3.organism)=upper(t2.organism)
						 minus
						 select pathway_id, to_char(gene_symbol), gene_id from de_pathway_gene
					 """
            }

            BioMarker bioMarker = new BioMarker()
            bioMarker.setBiomart(biomart);

            deapp.withTransaction {
                deapp.withBatch(1000, qry4, { ps ->
                    biomartuser.eachRow(qry1) 
                    {
                        GroovyRowResult rowResult = deapp.firstRow(qry2, [it.pathway])
                        if(rowResult != null) 
                        {
                            String dePathwayId = rowResult[0];
                            bioMarker.setOrganism(it.organism)
                            String extId = bioMarker.getBioMarkerExtID(it.gene_symbol, 'GENE')
                            if(extId == null) {
                                log.info "$it.gene_symbol 'GENE' not found in bio_marker for ${it.organism}"
                            }
                            else {
                                GroovyRowResult geneResult = deapp.firstRow(qry3, [dePathwayId, it.gene_symbol, extId])
                                int count = geneResult[0]
                                if(count > 0){
                                    log.info "$dePathwayId:$it.gene_symbol:$extId already exists in DE_PATHWAY_GENE ..."
                                }
                                else{
                                    log.info "Insert $dePathwayId:$it.gene_symbol:$extId into DE_PATHWAY_GENE ..."
                                    ps.addBatch([dePathwayId, it.gene_symbol, extId])
                                }
                            }
                        }
                    }
               })
            }
        }
        

	void loadPathwayGene(File pathwayData, Map pathwayId){

		String qry = "insert into de_pathway_gene(pathway_id, gene_symbol, gene_id) values(?, ?, ?)"

		String [] str = []
		if(pathwayData.exists()){

			if(isPathwayGeneSourceExist()){
				log.warn("Pathway Gene from $source already loaded into DE_PATHWAY_GENE")

				log.info("Start delete data for ${source} from DE_PATHWAY_GENE ...")

				String qry1 = "delete from de_pathway_gene where source=?"
				deapp.execute(qry1, [source])
			}

			log.info("Start loading " + pathwayData.toString() + " into DE_PATHWAY_GENE")

			deapp.withTransaction {
                            deapp.withBatch(1000, qry, { ps ->

					pathwayData.eachLine{
						str = it.split("\t")
						ps.addBatch([
							pathwayId[str[0]],
							str[2],
							str[1]
						])
					}
				})
			}
		}else{
			throw new RuntimeException("Cannot find file: " + pathwayData.toString())
		}
	}



	/**
	 * 
	 * @param geneAssociation		(pathway_id		gene_symbol)
	 * @param geneId				(gene_symbol	gene_id)
	 * @param pathwayId				(pathway_id		id in de_pathway)
	 */
	void loadPathwayGene(File geneAssociation, Map geneId, Map pathwayId){

		String qry = "insert into de_pathway_gene(pathway_id, gene_symbol, gene_id) values(?, ?, ?)"

		String [] str = []
		if(geneAssociation.exists()){

			if(isPathwayGeneExistSource()){
				log.info("Pathway Gene from $source already loaded into DE_PATHWAY_GENE")
			}else{
				log.info("Start loading " + geneAssociation.toString() + " into DE_PATHWAY_GENE")

				geneAssociation.eachLine{
					str = it.split("\t")

					if(!geneId[str[1].toUpperCase()].equals(null)){

                                            deapp.withBatch(1000, qry, { ps ->
							ps.addBatch([
								pathwayId[str[0]],
								str[1],
								geneId[str[1].toUpperCase()]
							])
						})
					}
				}
			}
		}else{
			throw new RuntimeException("Cannot find file: " + geneAssociation.toString())
		}
	}


    void loadPathwayGene(Sql deapp, File pathwayData){
		setDeapp(deapp)
		loadPathwayGene(pathwayData)
	}


    void loadPathwayGene(File pathwayData){
		String [] str = []
		if(pathwayData.exists()){
			pathwayData.eachLine{
				str = it.split("\t")
				//log.info str[0] + "\t" + str[1] + "\t" + str[2]
				insertPathwayGene(str[0], str[2], str[1])
			}
		}else{
			log.error("Cannot find Pathway Definition file: " + pathwayData.toString())
			throw new RuntimeException("Cannot find Pathway Definition file: " + pathwayData.toString())
		}
	}


    void insertPathwayGene(String pathwayId, String geneSymbol, String geneId){
		Map pathwayIdMap = getPathwayIdMap()
		String qry = "insert into de_pathway_gene(pathway_id, gene_symbol, gene_id) values(?,?,?)"

		if(isPathwayGeneExist(pathwayIdMap[pathwayId].toString(), geneId)){
			log.info "Pathway data for ($pathwayId, $geneSymbol, $geneId) already exists ..."
		}else{
			log.info "Loading pathway data ($pathwayId, $geneSymbol, $geneId) ..."
			deapp.execute(qry, [
				pathwayIdMap[pathwayId],
				geneSymbol,
				geneId
			])
		}
	}


	boolean isPathwayGeneExist(String pathwayId, String geneId){
		String qry = "select count(1) from de_pathway_gene where pathway_id =? and gene_id=?"
		if(deapp.firstRow(qry, [pathwayId, geneId])[0] > 0) return true
		else return false
	}


	boolean isPathwayGeneSourceExist(String source){
		String qry = "select count(1) from de_pathway_gene where pathway_id in (select id from de_pathway where source=?)"
		if(deapp.firstRow(qry, [source])[0] > 0) return true
		else return false
	}


	Map getPathwayIdMap(){
		Map pathwayMap = [:]
		String qry = "select id, externalid from de_pathway where source=?"
		deapp.eachRow(qry,[source]) {
			pathwayMap[it.externalid] = it.id
		}
		return pathwayMap
	}


	void setSource(String source){
		this.source = source
	}


	void setDeapp(Sql deapp){
		this.deapp = deapp
	}

	void setBiomart(Sql biomart){
		this.biomart = biomart
	}

	void setBiomartuser(Sql biomartuser){
		this.biomartuser = biomartuser
	}
}
