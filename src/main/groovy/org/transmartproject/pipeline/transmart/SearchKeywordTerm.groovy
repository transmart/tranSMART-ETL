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


package org.transmartproject.pipeline.transmart

import org.apache.log4j.Logger;

import groovy.sql.Sql;
import java.sql.SQLException;

import org.transmartproject.pipeline.util.Util


class SearchKeywordTerm {

    private static final Logger log = Logger.getLogger(SearchKeywordTerm)

    Sql searchapp

    def savedTerms = []

    void loadSearchKeywordTerm() {

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



    void insertSearchKeywordTerm(String keywordTerm, long searchKeywordId, int rank) {
        String keywordTermUpper = keywordTerm.toUpperCase()
        if (isSearchKeywordTermExist(keywordTermUpper, searchKeywordId)) {
            log.info "$keywordTermUpper:$searchKeywordId already exists in SEARCH_KEYWORD_TERM ..."
        } else {
            log.info "Save $keywordTermUpper:$searchKeywordId into SEARCH_KEYWORD_TERM ..."
            savedTerms.add([
                    keywordTermUpper,
                    searchKeywordId,
                    rank,
                    keywordTermUpper
            ])
            log.info "savedTerms "+savedTerms.size()
            if(savedTerms.size() >= 1000) {
                doInsertSearchKeywordTerms()
            }

        }
    }


    void doInsertSearchKeywordTerms() {
        String qry = """ insert into search_keyword_term(keyword_term, search_keyword_id, rank, term_length)
                         values(?, ?, ?, length(?)) """

        log.info "doInsertSearchKeywordTerms list size: "+savedTerms.size()
        try {
        searchapp.withTransaction {
            searchapp.withBatch(qry, {stmt ->
                savedTerms.each {
                    log.info "Insert ${it[0]}:${it[1]} into SEARCH_KEYWORD_TERM ..."
                    stmt.addBatch(it)
                }
           })
        }
        }
        catch (SQLException e) {
            def ee = e.getNextException()
            if(ee) {ee.printStackTrace()
            log.info "doInsertSearchKeywordTerms exception"
            }
        }
        finally
        {
            log.info "doInsertSearchKeywordTerms done"
        }
        
        savedTerms = []
    }


    boolean isSearchKeywordTermExist(String keywordTerm, long searchKeywordId) {
        String keywordTermUpper = keywordTerm.toUpperCase()
        String qry = """ select count(*) from search_keyword_term
		                 where keyword_term=? and search_keyword_id=?"""
        def res = searchapp.firstRow(qry, [keywordTermUpper, searchKeywordId])
        int count = res[0]
        return count > 0
    }


    void setSearchapp(Sql searchapp) {
        this.searchapp = searchapp
    }

    void closeSearchKeywordTerm(){
        int nterms = savedTerms.size()
        if(nterms > 0) {
            log.info "closeSearchKeywordTerm insert remaining $nterms terms"
            doInsertSearchKeywordTerms()
        }
    }
    
}
