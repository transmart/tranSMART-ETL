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


class SearchKeywordTerm {

	private static final Logger log = Logger.getLogger(SearchKeywordTerm)

	Sql searchapp

	void loadSearchKeywordTerm(){

		log.info "Start populating SEARCH_KEYWORD_TERM using data from SEARCH_KEYWORD ... "
		
		String qry = """ insert into search_keyword_term (KEYWORD_TERM, SEARCH_KEYWORD_ID, RANK,TERM_LENGTH)
	                 select upper(keyword), search_keyword_id, 1, length(keyword)
	 				 from search_keyword
	 				 where search_keyword_id not in
	 			 			(select search_keyword_id from searchapp.search_keyword_term)
	 			    """
		searchapp.execute(qry)
		
		log.info "End populating SEARCH_KEYWORD_TERM using data from SEARCH_KEYWORD ... "
	}


	
	void insertSearchKeywordTerm(String keyword, long searchKeywordId){

		String qry = """ insert into search_keyword_term(keyword_term,search_keyword_id,rank,term_length) values(?, ?, ?, length(?)) """

		if(isSearchKeywordTermExist(keyword, dataCategory)){
			log.info "$keyword:$dataCategory already exists in SEARCH_KERYWORD_TERM ..."
		}else{
			log.info "Insert $keyword:$dataCategory into SEARCH_KERYWORD_TERM ..."
			searchapp.execute(qry, [
				keyword,
				searchKeywordId,
				1,
				keyword
			])
		}
	}


	boolean isSearchKeywordTermExist(String keyword, String dataCategory){
		String qry = """ select count(*) from search_keyword_term 
		                 where search_keyword_id in 
							  (select search_keyword_id from search_keyword
		                       where keyword=? and data_category=?)"""
		def res = searchapp.firstRow(qry, [keyword, dataCategory])
		if(res[0] > 0) return true
		else return false
	}


	void setSearchapp(Sql searchapp){
		this.searchapp = searchapp
	}
}
