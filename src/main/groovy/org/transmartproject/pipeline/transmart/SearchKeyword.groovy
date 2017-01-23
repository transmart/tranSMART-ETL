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

import groovy.sql.GroovyRowResult
import org.apache.log4j.Logger;

import groovy.sql.Sql;
import org.transmartproject.pipeline.util.Util



class SearchKeyword {

    private static final Logger log = Logger.getLogger(SearchKeyword)

    Sql biomart
    Sql searchapp

    def savedKeys = []

    /**
     *   could come from either de_pathway or bio_marker
     *
     *   cleanup scripts:
     *
     *   	delete from search_keyword_term
     * 		where SEARCH_KEYWORD_ID in (
     * 		select search_keyword_id from search_keyword where data_category='PATHWAY');
     *
     * 		delete from search_keyword where data_category='PATHWAY';
     *
     */
    void loadPathwaySearchKeyword(String primarySourceCode) {

        log.info("Start loading search keyword for pathways '${primarySourceCode}'...")

        String qry = """ select distinct bio_marker_name, bio_marker_id, 
					 primary_source_code, organism,
                                         primary_external_id
			      from biomart.bio_marker
			      where bio_marker_type='PATHWAY'
                                and primary_source_code=?
			  """

        biomart.eachRow(qry,[primarySourceCode])
        {
            long bioMarkerId = it.bio_marker_id
            /* uniqueId is PATHWAY:KEGG:genus species:keggid */
            insertSearchKeyword(it.bio_marker_name, bioMarkerId,
                                'PATHWAY:'+it.primary_source_code+':'+it.organism+':'+it.primary_external_id,
                                it.primary_source_code, 'PATHWAY', 'Pathway')
/* version without organism if loading human only */
//            insertSearchKeyword(it.bio_marker_name, bioMarkerId,
//                                'PATHWAY:'+it.primary_source_code+':'+it.primary_external_id, // e.g. KEGG:hsa00000
//                                it.primary_source_code, 'PATHWAY', 'Pathway')
        }

        log.info "End loading search keyword for pathways '${primarySourceCode}'... "
    }


    void loadGeneSearchKeyword() {
        log.info "Start loading search keyword for all human genes ... "

/*
** Specific to human genes
**
** Test all genes in bio_marker and load as keywords
**
** we do not set a source - can be Entrez Gene Info, Pathway (KEGG, GO, Ingenuity)
** whichever is loaded first.
 */

        //String qry = "delete from search_keyword where data_category='GENE'"
        //searchapp.execute(qry)
        String qry;

        Boolean isPostgres = Util.isPostgres()

        if(isPostgres) {
            qry = """ select distinct bio_marker_name, bio_marker_id, primary_external_id
				  from biomart.bio_marker
				  where bio_marker_type='GENE' and organism='HOMO SAPIENS'"""
        } else 
        {
            qry = """ select distinct bio_marker_name, bio_marker_id, primary_external_id
				  from biomart.bio_marker
				  where bio_marker_type='GENE' and organism='HOMO SAPIENS'"""
        }
        
    
        biomart.eachRow(qry)
        {
            long bioMarkerId = it.bio_marker_id
            insertSearchKeyword(it.bio_marker_name, bioMarkerId,
                                'GENE:'+it.primary_external_id,
                                null, 'GENE', 'Gene')
        }


        log.info "End loading search keyword for all human genes ... "
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


    void insertSearchKeyword(String keyword, long bioDataId, String uniqueId,
                             String sourceCode, String dataCategory,
                             String displayDataCategory) {

        if (isSearchKeywordExist(keyword, dataCategory) || isSearchKeywordExistById(uniqueId, dataCategory) ) {
            //log.info "$keyword:$dataCategory:$bioDataId already exists in SEARCH_KEYWORD ..."
        } else {
            log.info "Save $keyword:$dataCategory:$bioDataId into SEARCH_KEYWORD ..."
            savedKeys.add([
                    keyword,
                    bioDataId,
                    uniqueId,
                    dataCategory,
                    sourceCode,
                    displayDataCategory
            ])
//            log.info "savedKeys "+savedKeys.size()
            if(savedKeys.size() >= 1) {
                doInsertSearchKeyword()
            }
        }
    }


    void doInsertSearchKeyword() {

        String qry = """ insert into search_keyword (keyword, bio_data_id,
                                           unique_id, data_category,
					   source_code, display_data_category)
                                     values(?, ?, ?, ?, ?, ?) """

        log.info "doInsertSearchKeyword list size: "+savedKeys.size()
        searchapp.withTransaction {
            searchapp.withBatch(qry, {stmt ->
                savedKeys.each {
                    log.info "savedKeys ${it}"
                    log.info "Insert ${it[0]}:${it[3]}:${it[1]} into SEARCH_KEYWORD ..."
                    searchapp.execute(qry, it)
                }
            })
        }
        savedKeys = []
    }


    boolean isSearchKeywordExistById(String uniqueId, String dataCategory) {
        String qry = "select count(*) from search_keyword where unique_id=? and data_category=?"
        GroovyRowResult rowResult = searchapp.firstRow(qry, [uniqueId, dataCategory])
        int count = rowResult[0]
        return count > 0
    }


    boolean isSearchKeywordExist(String keyword, String dataCategory) {
        String qry = "select count(*) from search_keyword where keyword=? and data_category=?"
        GroovyRowResult rowResult = searchapp.firstRow(qry, [keyword, dataCategory])
        int count = rowResult[0]
        return count > 0
    }


    boolean isSearchKeywordExist(String keyword, String dataCategory, long bioMarkerID) {
        String qry = "select count(*) from search_keyword where keyword=? and data_category=? and bio_data_id=?"
        GroovyRowResult rowResult = searchapp.firstRow(qry, [keyword, dataCategory, bioMarkerID])
        int count = rowResult[0]
        return count > 0
    }


    long getSearchKeywordId(String keyword, String dataCategory) {
        String qry = """ select search_keyword_id from search_keyword
						 where keyword=? and data_category=? """
        def res = searchapp.firstRow(qry, [keyword, dataCategory])
        if (res.equals(null)) {
            return 0
        } else {
            return res[0]
        }
    }

    void setBiomart(Sql biomart) {
        this.biomart = biomart
    }

    void setSearchapp(Sql searchapp) {
        this.searchapp = searchapp
    }

    void closeSearchKeyword(){
        int nkeys = savedKeys.size()
        if(nkeys > 0) {
            log.info "closeSearchKeyword insert remaining $nkeys keys"
            doInsertSearchKeyword()
        }
    }
    
}
