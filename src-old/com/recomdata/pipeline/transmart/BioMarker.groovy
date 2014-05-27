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

import org.apache.log4j.Logger;

import groovy.sql.Sql;

import com.recomdata.pipeline.util.Util


class BioMarker {

	private static final Logger log = Logger.getLogger(BioMarker)

	Sql biomart
	String organism

	void loadGenes(File gene){

		if(gene.size()>0){
			gene.eachLine {
				String [] str = it.split("\t")
				insertBioMarker(str[2], "", str[1], "", "GENE")
			}
		}else{
			log.info "Cannot find " + gene.toString()
		}
	}

	
	void loadGeneOntologyGenes(File gene, Map geneId){

		Map genes = [:]
		
		if(gene.size()>0){
			log.info "Start loading ${gene.toString()} into BIO_MARKER ... "
			gene.eachLine {
				String [] str = it.split("\t")
				genes[str[1].trim()] = 1
			}
			
			genes.each{ k, v ->
				if(!geneId[k.toString().toUpperCase()].equals(null)){
					insertBioMarker(k, "", (String) geneId[k.toString().toUpperCase()], "GO", "GENE")
				}
			}
		}else{
			log.info "Cannot find " + gene.toString()
		}
	}


	void loadPathways(File pathway, String source){

		if(pathway.size()>0){
			pathway.eachLine {
				String [] str = it.split("\t")
				insertBioMarker(str[1], str[1], str[0], source, "PATHWAY")
			}
		}else{
			log.info "Cannot find " + pathway.toString()
		}
	}


	void insertBioMarker(String geneSymbol, String description, String geneId, String source, String markerType){

		String qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism, primary_source_code,
		                        primary_external_id, bio_marker_type) values(?, ?, ?, ?, ?, ?) """

		if(isBioMarkerExist(geneId, markerType)){
			log.info "$organism:$geneSymbol:$geneId:$markerType already exists in BIO_MARKER ..."
		}else{
			log.info "Insert $organism:$geneSymbol:$geneId:$markerType into BIO_MARKER ..."
			biomart.execute(qry, [
				geneSymbol,
				description,
				organism,
				source,
				geneId,
				markerType
			])
		}
	}


	boolean isBioMarkerExist(String geneId, String markerType){
		String qry = "select count(*) from bio_marker where primary_external_id=? and organism=? and bio_marker_type=?"
		def res = biomart.firstRow(qry, [geneId, organism, markerType])
		if(res[0] > 0) return true
		else return false
	}


	void setOrganism(String organism){
		this.organism = organism
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
