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


class SearchKeyword {

	private static final Logger log = Logger.getLogger(SearchKeyword)

	Sql searchapp
	String dataCategory, sourceCode, displayDataCategory

	/**
	 *   could come from either de_pathway or bio_marker 
	 *   
	 *   cleanup scripts:
	 *   
	 *   	delete from search_keyword_term 
	 *		where SEARCH_KEYWORD_ID in ( 
	 *		select search_keyword_id from search_keyword where data_category='PATHWAY');
	 *
	 *		delete from search_keyword where data_category='PATHWAY';
	 *   
	 */
	void loadPathwaySearchKeyword(String primarySourceCode) {

		log.info "Start loading search keyword for pathways ... "

		String qry = """ insert into search_keyword (keyword, bio_data_id, unique_id, data_category,
							   source_code, display_data_category)
   				  select distinct bio_marker_name, bio_marker_id, 
						'PATHWAY:'||primary_source_code||':'||organism||':'||primary_external_id, 
   				        'PATHWAY', primary_source_code, 'Pathway'
			      from biomart.bio_marker
			      where bio_marker_type='PATHWAY' and primary_source_code=?
					  and 'PATHWAY:'||primary_source_code||':'||upper(organism)||':'||primary_external_id not in
			          	(select to_char(upper(unique_id)) from search_keyword where data_category='PATHWAY')
			  """
		searchapp.execute(qry, [primarySourceCode])

		log.info "End loading search keyword for pathways ... "
	}


	void loadGeneSearchKeyword() {

		log.info "Start loading search keyword for genes ... "

		//String qry = "delete from search_keyword where data_category='GENE'"
		//searchapp.execute(qry)

		String qry = """ insert into search_keyword (keyword, bio_data_id, unique_id, data_category,
							   source_code, display_data_category)
				  select distinct bio_marker_name, bio_marker_id, 'GENE:'||primary_external_id, 'GENE', '', 'Gene'
				  from biomart.bio_marker
				  where bio_marker_type='GENE' and upper(organism)='HOMO SAPIENS' and 'GENE:'||primary_external_id not in 
				         (select to_char(unique_id) from search_keyword where data_category='GENE')
		      """
		searchapp.execute(qry)

		log.info "Start loading search keyword for genes ... "
	}


	void loadOmicsoftGSESearchKeyword(String biomart) {

		log.info "Start deleting search keyword for Omicsoft GSEs ... "

		String qry = """ delete from search_keyword where data_category='STUDY' and 
									display_data_category='GEO' and  keyword like 'GSE%' """
		//searchapp.execute(qry)


		log.info "Start inserting search keyword for Omicsoft GSEs ... "

		qry = """ insert into search_keyword (keyword, bio_data_id, unique_id, data_category, display_data_category)
				  select distinct accession, bio_experiment_id, 'Omicsoft: '||accession, 'STUDY', 'GEO'
				  from ${biomart}.bio_experiment
				  where accession not in (select keyword from search_keyword)
			  """
		searchapp.execute(qry)

		log.info "Start loading search keyword for Omicsoft GSEs ... "
	}


	void loadOmicsoftCompoundSearchKeyword() {

		log.info "Start deleting search keyword for Omicsoft compounds ... "

		String qry = """ delete from search_keyword where data_category='COMPOUND' and source_code='Omicsoft' """
		//searchapp.execute(qry)


		log.info "Start inserting search keyword for Omicsoft compounds ... "

		qry = """ insert into search_keyword (bio_data_id, keyword, unique_id, data_category, display_data_category, source_code)
				  select t2.bio_compound_id, t1.code_name, 'COM:'||t1.cas_registry, 'COMPOUND', 'Compound', 'Omicsoft'
				  from ${biomart}.bio_compound t1, ${biomart}.bio_data_compound t2
				  where t1.bio_compound_id=t2.bio_compound_id and t2.etl_source='OMICSOFT'
					   and t1.code_name not in (select keyword from search_keyword)
			  """
		searchapp.execute(qry)

		log.info "Start loading search keyword for Omicsoft compounds ... "
	}


	void loadOmicsoftDiseaseSearchKeyword(String biomart) {

		log.info "Start deleting search keyword for Omicsoft diseases ... "

		String qry = """ delete from search_keyword where data_category='DISEASE' and source_code='Omicsoft' """
		//searchapp.execute(qry)


		log.info "Start inserting search keyword for Omicsoft diseases ... "

		qry = """ insert into search_keyword (bio_data_id, keyword, unique_id, data_category, display_data_category, source_code)
				  select distinct t2.bio_disease_id, t1.disease, 'DIS:'||t1.mesh_code, 'DISEASE', 'Disease', ''
				  from ${biomart}.bio_disease t1, ${biomart}.bio_data_disease t2
				  where t1.bio_disease_id=t2.bio_disease_id and t2.etl_source='OMICSOFT'
						and t1.disease not in (select keyword from search_keyword where data_category='DISEASE')
			  """
		searchapp.execute(qry)

		log.info "End loading search keyword for Omicsoft diseases ... "
	}

	
	void loadOmicsoftCompoundSearchKeyword(String biomart) {

		log.info "Start deleting search keyword for Omicsoft diseases ... "
		//searchapp.execute(qry)

		log.info "End loading search keyword for Omicsoft diseases ... "
	}

	
	void insertSearchKeyword(String keyword, long bioDataId, String externalId){

		String uniqueId = ""
		String qry = """ insert into search_keyword (keyword, bio_data_id, unique_id, data_category,
							   source_code, display_data_category) values(?, ?, ?, ?, ?, ?) """

		if(sourceCode.equals(null)) uniqueId = dataCategory + ":" + externalId
		else uniqueId = dataCategory + ":" + displayDataCategory + ":" + externalId

		if(isSearchKeywordExist(keyword)){
			log.info "$keyword:$dataCategory already exists in SEARCH_KEYWORD ..."
		}else{
			log.info "Insert $keyword:$dataCategory into SEARCH_KEYWORD ..."
			searchapp.execute(qry, [
				keyword,
				bioDataId,
				uniqueId,
				dataCategory,
				sourceCode,
				displayDataCategory
			])
		}
	}


	boolean isSearchKeywordExist(String keyword){
		String qry = "select count(*) from search_keyword where keyword=? and data_category=?"
		def res = searchapp.firstRow(qry, [keyword, dataCategory])
		if(res[0] > 0) return true
		else return false
	}


	long getSearchKeywordId(String keyword){
		String qry = """ select search_keyword_id from search_keyword 
						 where keyword=? and data_category=? """
		def res = searchapp.firstRow(qry, [keyword, dataCategory])
		if(res.equals(null)) return 0
		else return res[0]
	}


	void setDisplayDataCategory(String displayDataCategory){
		this.displayDataCategory = displayDataCategory
	}


	void setSourceCode(String sourceCdoe){
		this.sourceCode = sourceCdoe
	}


	void setDataCategory(String dataCategory){
		this.dataCategory = dataCategory
	}


	void setSearchapp(Sql searchapp){
		this.searchapp = searchapp
	}
}
