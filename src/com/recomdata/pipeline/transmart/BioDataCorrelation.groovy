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


class BioDataCorrelation {

	private static final Logger log = Logger.getLogger(BioDataCorrelation)

	Sql biomart
	long bioDataCorrelDescrId
	String organism, source

	void loadBioDataCorrelation(File goa, Map geneId){

		String qry = """ insert into bio_data_correlation(
								bio_data_id, asso_bio_data_id, bio_data_correl_descr_id)
						 select distinct p.bio_marker_id,
								g.bio_marker_id,
								c.bio_data_correl_descr_id
						 from bio_marker p, bio_marker g, bio_data_correl_descr c
						 where p.bio_marker_type = 'PATHWAY' and g.bio_marker_type = 'GENE' and
							   p.primary_external_id = ? and g.primary_external_id = ? and
							   c.correlation='PATHWAY GENE' and g.organism=? """
		if(goa.size()>0){

			log.info ("Start loading ${goa.toString()} into BIO_DATA_CORRELATION ...")

			goa.eachLine{
				String [] str = it.split("\t")
				//log.info "insert (${str[0]}, ${str[1]}, $organism) into BIO_DATA_CORRELATION ..."
				biomart.execute(qry, [
					str[0],
					(String) geneId[str[1].toUpperCase()],
					organism
				])
			}
		} else {
			log.info "Cannot open the file:" + goa.toString()
		}
	}


	void loadBioDataCorrelation(File bdc){

		String qry = """ insert into bio_data_correlation(
		                        bio_data_id, asso_bio_data_id, bio_data_correl_descr_id)
						 select p.bio_marker_id, 
						        g.bio_marker_id, 
						        c.bio_data_correl_descr_id
						 from bio_marker p, bio_marker g, bio_data_correl_descr c
						 where p.bio_marker_type = 'PATHWAY' and g.bio_marker_type = 'GENE' and 
						       p.primary_external_id = ? and g.primary_external_id = ? and 
						       c.correlation='PATHWAY GENE' and upper(g.organism)=upper(?) 
						 minus
                         select bio_data_id, asso_bio_data_id, bio_data_correl_descr_id 
						 from bio_data_correlation """
		if(bdc.size()>0){
			bdc.eachLine{
				String [] str = it.split("\t")
				log.info "insert (${str[0]}, ${str[1]}, $organism) into BIO_DATA_CORRELATION ..."
				biomart.execute(qry, [str[0], str[1], organism])
			}
		} else {
			log.info "Cannot open the file:" + bdc.toString()
		}
	}


	void loadBioDataCorrelation(String pathwayDataTable){

		log.info ("Start populating bio_data_correlation using table ${pathwayDataTable} ...")

		String qry = """ insert into biomart.bio_data_correlation(
								bio_data_id, asso_bio_data_id, bio_data_correl_descr_id)
						 select p.bio_marker_id, g.bio_marker_id, c.bio_data_correl_descr_id
						 from biomart.bio_marker p, biomart.bio_marker g, 
								biomart.bio_data_correl_descr c, ${pathwayDataTable} t
						 where p.bio_marker_type = 'PATHWAY' and g.bio_marker_type = 'GENE' and
							   p.primary_external_id = t.pathway and upper(g.bio_marker_name) = upper(t.gene_symbol) and
							   c.correlation='PATHWAY GENE' and upper(g.organism)=upper(t.organism) 
							   and upper(p.organism)=upper(t.organism) 
						 minus
						 select bio_data_id, asso_bio_data_id, bio_data_correl_descr_id
						 from biomart.bio_data_correlation"""

		biomart.execute(qry)

		log.info ("End populating bio_data_correlation using table ${pathwayDataTable} ...")
	}



	void insertBioDataCorrelation(long pathwayMarkerId, long geneMarkerId, long dataCorrelDecrId){

		String qry = "insert into bio_data_correlation(bio_data_id,asso_bio_data_id,bio_data_correl_descr_id) values(?,?,?)"

		if(isBioDataCorrelationExist(pathwayMarkerId, geneMarkerId, dataCorrelDecrId)){
			log.info "$pathwayMarkerId:$geneMarkerId:$dataCorrelDecrId already exists in BIO_MARKER ..."
		}else{
			log.info "Insert $pathwayMarkerId:$geneMarkerId:$dataCorrelDecrId into BIO_MARKER ..."
			biomart.execute(qry, [
				pathwayMarkerId,
				geneMarkerId,
				dataCorrelDecrId
			])
		}
	}


	boolean isBioDataCorrelationExist(String pathway, String geneId){
		String qry = """ select count(*) 
		                 from bio_data_correlation c, bio_marker p, bio_marker g, bio_data_correl_descr d
						 where p.bio_marker_type = 'PATHWAY' and g.bio_marker_type = 'GENE' and 
						       p.primary_external_id = ? and g.primary_external_id = ? and 
						       d.correlation='PATHWAY GENE' and c.bio_data_id=p.bio_marker_id and 
						       c.asso_bio_data_id = g.bio_marker_id """
		def res = biomart.firstRow(qry, [pathway, geneId])
		if(res[0] > 0) return true
		else return false
	}


	boolean isBioDataCorrelationExist(long pathwayMarkerId, long geneMarkerId, long dataCorrelDecrId){
		String qry = """ select count(*) from bio_data_correlation 
		                 where bio_data_id=? and asso_bio_data_id=? and bio_data_correl_descr_id=? """
		def res = biomart.firstRow(qry, [
			pathwayMarkerId,
			geneMarkerId,
			dataCorrelDecrId
		])
		if(res[0] > 0) return true
		else return false
	}


	void setBioDataCorrelDescrId(long bioDataCorrelDescrId){
		this.bioDataCorrelDescrId = bioDataCorrelDescrId
	}


	void setOrganism(String organism){
		this.organism = organism
	}


	void setSource(String sourcec){
		this.source = sourcec
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
