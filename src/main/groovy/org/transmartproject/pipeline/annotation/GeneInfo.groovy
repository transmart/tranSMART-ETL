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
  

package org.transmartproject.pipeline.annotation

import java.util.Properties;

import org.transmartproject.pipeline.transmart.SearchKeyword
import org.transmartproject.pipeline.transmart.SearchKeywordTerm
import org.transmartproject.pipeline.util.Util

import groovy.sql.Sql;
import groovy.sql.GroovyRowResult
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator

class GeneInfo {

	private static final Logger log = Logger.getLogger(GeneInfo)

        private static SearchKeyword searchKeyword
        private static SearchKeywordTerm searchKeywordTerm

	Sql biomart
	Sql searchapp
	String geneInfoTable, geneSynonymTable

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()
		Properties props = Util.loadConfiguration("conf/GeneInfo.properties")

		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

                searchKeyword = new SearchKeyword()
                searchKeyword.setSearchapp(searchapp)

                searchKeywordTerm = new SearchKeywordTerm()
                searchKeywordTerm.setSearchapp(searchapp)

		if(props.get("skip_load_gene_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Gene Info ..."
		}else{
			File geneInfo = new File(props.get("gene_info_source"))

			// store Human (9606), Mouse (10090), and Rat (10116) data
			File entrez = new File(props.get("gene_info_source") + ".tsv")
			if(entrez.size() >0 ){
				entrez.delete()
				entrez.createNewFile()
			}

			File synonym = new File(props.get("gene_info_source") + ".synonym")
			if(synonym.size() >0){
				synonym.delete()
				synonym.createNewFile()
			}

			GeneInfo gi = new GeneInfo()
			gi.setBiomart(biomart)
			gi.setGeneInfoTable(props.get("gene_info_table"))
			gi.setGeneSynonymTable(props.get("gene_synonym_table"))

			if(props.get("create_gene_info_table").toString().toLowerCase().equals("yes")){
				gi.createGeneInfoTable()
			}else{
				log.info "Skip creating table "+props.get("gene_info_table")+" ..."
			}

			if(props.get("create_gene_synonym_table").toString().toLowerCase().equals("yes")){
				gi.createGeneSynonymTable()
			}else{
				log.info "Skip creating table "+props.get("create_gene_synonym_table")+" ..."
			}

			Map selectedOrganism = gi.getSelectedOrganism(props.get("selected_organism"))
			gi.extractSelectedGeneInfo(geneInfo, entrez, synonym, selectedOrganism)
			//gi.readGeneInfo(geneInfo, entrez, synonym)
			gi.loadGeneInfo(entrez)
			gi.updateBioMarker(selectedOrganism)
			gi.loadGeneSynonym(synonym)
			gi.updateBioDataUid(selectedOrganism)
			gi.updateBioDataExtCode(selectedOrganism)

                        if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
                            log.info("Skip loading Entrez GeneInfo annotation from ${gi.geneInfoTable} to SEARCH_KEYWORD ...")
                        }else{
                            gi.loadSearchKeyword(searchapp,biomart,gi.geneInfoTable,gi.geneSynonymTable,selectedOrganism)
                            searchKeyword.closeSearchKeyword()
                            searchKeywordTerm.closeSearchKeywordTerm()
                        }

                        print new Date()
                        println " Entrez GeneInfo annotation load completed successfully"
		}
	}



	void updateBioMarker(Map selectedOrganism){
		selectedOrganism.each{taxonomyId, organism ->
			updateBioMarker(taxonomyId, organism)
		}
	}


	void updateBioMarker(String taxonomyId, String organism){

        Boolean isPostgres = Util.isPostgres()
        String qry;
		log.info "Start updating BIO_MARKER for $taxonomyId:$organism using Entrez data ..."

        if(isPostgres){
		qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism, primary_source_code,
								primary_external_id, bio_marker_type)
						 select gene_symbol, gene_descr, ?, 'Entrez', gene_id::text, 'GENE'
						 from ${geneInfoTable}
						 where tax_id=? and gene_id::text not in
							 (select primary_external_id from bio_marker where organism=?) """
        } else {
		qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism, primary_source_code,
								primary_external_id, bio_marker_type)
						 select gene_symbol, gene_descr, ?, 'Entrez', to_char(gene_id), 'GENE'
						 from ${geneInfoTable}
						 where tax_id=? and to_char(gene_id) not in
							 (select primary_external_id from bio_marker where organism=?) """
        }

		biomart.execute(qry, [organism, taxonomyId, organism])

		log.info "End updating BIO_MARKER for $taxonomyId:$organism using Entrez data ..."
	}



	// can be retired
	void updateBioMarker(){
        Boolean isPostgres = Util.isPostgres()
        String qry;

		log.info "Start updating BIO_MARKER using Entrez data ..."

        if(isPostgres){
		qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism, primary_source_code,
		                        primary_external_id, bio_marker_type)
					     select gene_symbol, gene_descr, ?, 'Entrez', gene_id::text, 'GENE'
						 from ${geneInfoTable}
						 where tax_id=? and gene_id::text not in
						 	(select primary_external_id from bio_marker where organism=?) """
        } else {
		qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism, primary_source_code,
		                        primary_external_id, bio_marker_type)
					     select gene_symbol, gene_descr, ?, 'Entrez', to_char(gene_id), 'GENE'
						 from ${geneInfoTable}
						 where tax_id=? and to_char(gene_id) not in
						 	(select primary_external_id from bio_marker where organism=?) """
        }

		log.info "Start updating Home sapiens gene info  ..."
		biomart.execute(qry, [
			"Homo sapiens",
			"9606",
			"HOMO SAPIENS"
		])
		log.info "End updating Home sapiens gene info  ..."

		log.info "Start updating Mus musculus gene info  ..."
		biomart.execute(qry, [
			"Mus musculus",
			"10090",
			"MUS MUSCULUS"
		])
		log.info "End updating Mus musculus gene info  ..."

		log.info "Start updating Rattus norvegicus gene info  ..."
		biomart.execute(qry, [
			"Rattus norvegicus",
			"10116",
			"RATTUS NORVEGICUS"
		])
		log.info "End updating Rattus norvegicus gene info  ..."

		log.info "End updating BIO_MARKER using Entrez data ..."
	}

	
	/**
	*
	* @param selectedOrganism
	*/
   void updateBioDataUid(Map selectedOrganism){
	   selectedOrganism.each{ taxonomyId, organism ->
		   updateBioDataUid(taxonomyId, organism)
	   }
   }

   
   void updateBioDataUid(String taxonomyId, String organism){

        Boolean isPostgres = Util.isPostgres()
        String qry1
        String qry2
        String qry3

        log.info "Start loading BIO_DATA_UID using Entrez data ..."

        if(isPostgres){
            qry1 = """ select bio_marker_id, primary_external_id
		       from bio_marker where organism=? """
            qry2 = """ select count(*) from bio_data_uid where bio_data_id=? or unique_id=? """
            qry3 = """ insert into bio_data_uid(bio_data_id, unique_id, bio_data_type) values(?,?,?)"""
        } else {
            qry1 = """ select bio_marker_id, primary_external_id
		       from bio_marker where organism=? """
            qry2 = """ select count(*) from bio_data_uid where bio_data_id=? or unique_id=? """
            qry3 = """ insert into bio_data_uid(bio_data_id, unique_id, bio_data_type) values(?,?,?)"""
//	   qry = """ insert into bio_data_uid(bio_data_id, unique_id, bio_data_type)
//								select bio_marker_id, 'GENE:'||primary_external_id, to_nchar('BIO_MARKER.GENE')
//								from biomart.bio_marker where organism=?
//								minus
//								select bio_data_id, unique_id, bio_data_type
//								from bio_data_uid """
        }
        
        log.info "Start loading genes from $taxonomyId:$organism  ..."

        biomart.withTransaction {
            biomart.withBatch(1000, qry3, { ps ->
                biomart.eachRow(qry1,[organism])
                {
                    String uniqueId = 'GENE:'+it.primary_external_id
                    GroovyRowResult rowResult = biomart.firstRow(qry2, [it.bio_marker_id, uniqueId])
                    int count = rowResult[0]
                    if(count > 0){
                        //log.info "$organism:$it.bio_marker_id:$uniqueId already exists ($count) in BIO_DATA_UID ..."
                    }
                    else{
                        log.info "Insert $organism:$it.bio_marker_id:$uniqueId into BIO_DATA_UID ..."
                        ps.addBatch([it.bio_marker_id, uniqueId, 'BIO_MARKER.GENE'])
                    }
                }
            })
        }
        
        log.info "End loading genes from $taxonomyId:$organism  ..."

        log.info "End loading BIO_DATA_UID using Entrez data ..."
   }
   

	/**
	 * 
	 * @param selectedOrganism
	 */
	void updateBioDataExtCode(Map selectedOrganism){
		selectedOrganism.each{ taxonomyId, organism -> 
			updateBioDataExtCode(taxonomyId, organism)
		}
	}

	
	void updateBioDataExtCode(String taxonomyId, String organism){

        Boolean isPostgres = Util.isPostgres()
        String qry;
		log.info "Start loading BIO_DATA_EXT_CODE using Entrez's synonyms data ..."

        if(isPostgres){
		qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
								 select t2.bio_marker_id, t1.gene_synonym, 'Alias', 'SYNONYM', 'BIO_MARKER.GENE'
								 from ${geneSynonymTable} t1, bio_marker t2
								 where tax_id=? and t1.gene_id::text = t2.primary_external_id
									  and t2.organism=?
								 except
								 select bio_data_id, code, code_source::text, code_type::text, bio_data_type
								 from bio_data_ext_code"""
        } else {
		qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
								 select t2.bio_marker_id, t1.gene_synonym, 'Alias', 'SYNONYM', 'BIO_MARKER.GENE'
								 from ${geneSynonymTable} t1, bio_marker t2
								 where tax_id=? and  to_char(t1.gene_id) = t2.primary_external_id
									  and t2.organism=?
								 minus
								 select bio_data_id, code, to_char(code_source), to_char(code_type), bio_data_type
								 from bio_data_ext_code """
        }
        
		log.info "Start loading synonyms for genes from $taxonomyId:$organism  ..."
		biomart.execute(qry, [taxonomyId, organism])
		log.info "End loading synonyms for genes from $taxonomyId:$organism  ..."

		log.info "End loading BIO_DATA_EXT_CODE using Entrez's synonyms data ..."
	}


	// can be retired
	void updateBioDataExtCode(){
        Boolean isPostgres = Util.isPostgres()
        String qry;

		log.info "Start loading BIO_DATA_EXT_CODE using Entrez's Synonyms data ..."

        if(isPostgres){
		qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
						 select t2.bio_marker_id, t1.gene_synonym, 'Alias', 'SYNONYM', 'BIO_MARKER.GENE'
						 from ${geneSynonymTable} t1, bio_marker t2
						 where tax_id=? and to_t1.gene_id::text = t2.primary_external_id 
							  and t2.organism=? 
						 except
						 select bio_data_id, code, code_source::text, code_type::text, bio_data_type 
						 from bio_data_ext_code """
        } else {
		qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
						 select t2.bio_marker_id, t1.gene_synonym, 'Alias', 'SYNONYM', 'BIO_MARKER.GENE'
						 from ${geneSynonymTable} t1, bio_marker t2
						 where tax_id=? and to_char(t1.gene_id) = t2.primary_external_id 
							  and t2.organism=? 
						 minus
						 select bio_data_id, code, to_char(code_source), to_char(code_type), bio_data_type 
						 from bio_data_ext_code """
        }
        
		log.info "Start loading synonyms for Home sapiens genes  ..."
		biomart.execute(qry, [
			"9606",
			"HOMO SAPIENS"
		])
		log.info "End loading synonyms for Home sapiens genes  ..."

		log.info "Start loading synonyms for Mus musculus genes  ..."
		biomart.execute(qry, [
			"10090",
			"MUS MUSCULUS"
		])
		log.info "End loading synonyms for Mus musculus genes  ..."

		log.info "Start loading synonyms for Rattus norvegicus genes  ..."
		biomart.execute(qry, [
			"10116",
			"RATTUS NORVEGICUS"
		])
		log.info "End loading synonyms for Rattus norvegicus genes  ..."

		log.info "End loading BIO_DATA_EXT_CODE using Entrez'S Synonyms data ..."
	}


	/**
	 * 
	 * 0	tax_id		the unique identifier provided by NCBI Taxonomy for the species or strain/isolate
	 * 1	GeneID		the unique identifier for a gene ASN1:  geneid
	 * 2	Symbol		the default symbol for the gene ASN1:  gene->locus
	 * 3	LocusTag
	 * 4	Synonyms
	 * 5	dbXrefs
	 * 6	chromosome
	 * 7	map_location
	 * 8	description
	 * 9	type_of_gene
	 * 10	Symbol_from_nomenclature_authority
	 * 11	Full_name_from_nomenclature_authority
	 * 12	Nomenclature_status
	 * 13	Other_designations
	 * 14	Modification_date
	 * 
	 * @param geneInfo
	 */

	void readGeneInfo(File geneInfo, File entrez, File synonym){

		StringBuffer sb = new StringBuffer()
		StringBuffer sbSynonym = new StringBuffer()

		if(geneInfo.size() > 0){
			log.info "Reading Gene Info file: " + geneInfo.toString()
			geneInfo.eachLine {
				String [] str = it.split(/\t/)
				if(it.indexOf("#Format") !=  -1){
					String [] s = it.replace("#Format: ", "").split(" ")
					//for(int i in 0 .. s.size()-1) println i + "\t" + s[i]
				}else{
					if((str[0].indexOf("9606") == 0) || (str[0].indexOf("10090") == 0) || (str[0].indexOf("10116") == 0)) {
						sb.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[8] + "\n")

						if(!str[4].equals("-")){
							println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[4]
							if(str[4].indexOf("|") != -1) {
								String [] tmp = str[4].split(/\|/)
								tmp.each{
									if(!it.equals(null) && (it.trim().size() > 0))
										sbSynonym.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + it + "\n")
									println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + it
								}
							}else{
								sbSynonym.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[4] + "\n")
							}
						}
					}
				}
			}
		}else{
			log.error(geneInfo.toString() + " is empty.")
			return
		}

		if(entrez.size() >0){
			entrez.delete()
			entrez.createNewFile()
		}
		if(sb.size() > 0) entrez.append(sb.toString())

		if(synonym.size() >0){
			synonym.delete()
			synonym.createNewFile()
		}
		if(sbSynonym.size() > 0)  synonym.append(sbSynonym.toString())
	}



	Map getSelectedOrganism(String selectedOrganism){

		Map selectedOrganismMap = [:]

		if(selectedOrganism.indexOf(";")){
			String [] oragnisms = selectedOrganism.split(";")
			for(int n in 0 .. oragnisms.size()-1){
				String [] temp = oragnisms[n].split(":")
				selectedOrganismMap[temp[0]] = temp[1]
			}
		}else{
			selectedOrganismMap[selectedOrganism.split(":")[0]] = selectedOrganism.split(":")[1]
		}

		return selectedOrganismMap
	}



	void extractSelectedGeneInfo(File geneInfo, File entrez, File synonym, Map selectedOrganism){
		selectedOrganism.each{k, v ->
			extractSelectedGeneInfo(geneInfo, entrez, synonym, k, v)
		}
	}



	void extractSelectedGeneInfo(File geneInfo, File entrez, File synonym, String taxonomyId, String organism){

		StringBuffer sb = new StringBuffer()
		StringBuffer sbSynonym = new StringBuffer()

		if(geneInfo.size() > 0){
			log.info "Extracting data for $taxonomyId:$organism from Gene Info file: " + geneInfo.toString()
			geneInfo.eachLine {
				String [] str = it.split(/\t/)
				if(it.indexOf("#Format") !=  -1){
					String [] s = it.replace("#Format: ", "").split(" ")
					//for(int i in 0 .. s.size()-1) println i + "\t" + s[i]
				}else{
					if(str[0] == taxonomyId) {
						sb.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[8] + "\n")

						if(!str[4].equals("-")){
							// println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[4]
							if(str[4].indexOf("|") != -1) {
								String [] tmp = str[4].split(/\|/)
								tmp.each{
									if(!it.equals(null) && (it.trim().size() > 0))
										sbSynonym.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + it + "\n")
									//println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + it
								}
							}else{
								sbSynonym.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[4] + "\n")
							}
						}
					}
				}
			}
		}else{
			log.error(geneInfo.toString() + " is empty.")
			return
		}

		if(sb.size() > 0) entrez.append(sb.toString())
		if(sbSynonym.size() > 0)  synonym.append(sbSynonym.toString())
	}



	void loadGeneInfo(File geneInfo){

		String qry = "insert into $geneInfoTable (tax_id, gene_id, gene_symbol, gene_descr) values (?, ?, ?, ?)"

		if(geneInfo.size() > 0){
			log.info "Start loading file: " + geneInfo.toString()
			biomart.withTransaction {
				biomart.withBatch(100, qry,  { stmt ->
					geneInfo.eachLine {

						String [] str = it.split(/\t/)
						//println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[3]

                                                try {
                                                    stmt.addBatch([
                                                        str[0].trim().toInteger(),
                                                        str[1].trim().toInteger(),
                                                        str[2].trim(),
                                                        str[3].trim()
                                                    ])
                                                }
                                                catch (SQLException e)  {
                                                    def ee = e.getNextException()
                                                    if(ee) {
                                                        ee.printStackTrace()
                                                        log.info "extractSelectedGeneInfo exception"
                                                    }
                                                }
                                                
					}
				})
			}
		}else{
			log.error("Gene Info file is empty.")
			return
		}

		log.info "End loading Gene Info file: " + geneInfo.toString()
	}


	void loadGeneSynonym(File geneSynonym){

		String qry = "insert into $geneSynonymTable (tax_id, gene_id, gene_symbol, gene_synonym) values (?, ?, ?, ?)"

		if(geneSynonym.size() > 0){
			log.info "Start loading file: " + geneSynonym.toString()
			biomart.withTransaction {
				biomart.withBatch(100, qry,  { stmt ->
					geneSynonym.eachLine {

						String [] str = it.split(/\t/)
						//println str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[3]

						stmt.addBatch([
                                                        str[0].trim().toInteger(),
							str[1].trim().toInteger(),
							str[2].trim(),
							str[3].trim()
						])
					}
				})
			}
		}else{
			log.error("Gene's synonym file is empty.")
			return
		}

		log.info "End loading gene synonym file: " + geneSynonym.toString()
	}


	void createGeneInfoTable(){

        Boolean isPostgres = Util.isPostgres()

        String qry
        String qry1
        String qry2

        if(isPostgres) {
            qry1 = "select count(1) from pg_tables where tablename=?"
            qry2 = "drop table $geneInfoTable"

            if(biomart.firstRow(qry1, [geneInfoTable])[0] > 0){
                log.info "Drop table $geneInfoTable ..."
                biomart.execute(qry2)
            }
        } else {
            qry1 = "select count(1) from user_tables where table_name=?"
            qry2 = "drop table $geneInfoTable purge"

            if(biomart.firstRow(qry1, [geneInfoTable.toUpperCase()])[0] > 0){
                log.info "Drop table $geneInfoTable ..."
                biomart.execute(qry2)
            }
        }

		log.info "Start creating table $geneInfoTable ..."

                if(isPostgres){
                    qry = """ create table $geneInfoTable (
						tax_id   numeric(10),
						gene_id   numeric(20),
						gene_symbol   character varying(200),
						gene_descr    character varying(4000)
				 ) """
                } else {
                    qry = """ create table $geneInfoTable (
						tax_id   number(10,0),
						gene_id   number(20,0),
						gene_symbol   varchar2(200),
						gene_descr    varchar2(4000)
				 ) """
                }
                
		biomart.execute(qry)

		log.info "End creating table $geneInfoTable ..."
	}


	void createGeneSynonymTable(){

        Boolean isPostgres = Util.isPostgres()

        String qry
        String qry1
        String qry2

        if(isPostgres) {
            qry1 = "select count(1) from pg_tables where tablename=?"
            qry2 = "drop table $geneSynonymTable"

            if(biomart.firstRow(qry1, [geneSynonymTable])[0] > 0){
                log.info "Drop table $geneSynonymTable ..."
                biomart.execute(qry2)
            }
        } else {
            qry1 = "select count(1) from user_tables where table_name=?"
            qry2 = "drop table $geneSynonymTable purge"

            if(biomart.firstRow(qry1, [geneSynonymTable.toUpperCase()])[0] > 0){
                log.info "Drop table $geneSynonymTable ..."
                biomart.execute(qry2)
            }
        }

		log.info "Start creating table $geneSynonymTable ..."

               if(isPostgres){
		qry = """ create table $geneSynonymTable (
								tax_id        numeric(10),
								gene_id       numeric(20),
								gene_symbol   character varying(200),
								gene_synonym  character varying(200)
						 ) """
               } else {
		qry = """ create table $geneSynonymTable (
								tax_id        number(10,0),
								gene_id       number(20,0),
								gene_symbol   varchar2(200),
								gene_synonym       varchar2(200)
						 ) """
               }
               

		biomart.execute(qry)

		log.info "End creating table $geneSynonymTable ..."
	}


    void loadSearchKeyword(Sql searchapp, Sql biomart, String geneInfoTable, String geneSynonymTable, Map selectedOrganism){
        Boolean isPostgres = Util.isPostgres()
        String qry;
        String qrysyn;

        if(isPostgres) {
            qry = """ select distinct t1.gene_id, t1.gene_symbol, t2.bio_marker_id
                             from ${geneInfoTable} t1, biomart.bio_marker t2
                             where tax_id=? and t1.gene_id = t2.bio_marker_id"""
            qrysyn = """ select distinct gene_synonym
                                 from ${geneSynonymTable}
			         where tax_id=? and gene_symbol=?""" 
        } else {
            qry = """ select distinct t1.gene_id, t1.gene_symbol, t2.bio_marker_id
                             from ${geneInfoTable} t1, biomart.bio_marker t2
                             where tax_id=? and t1.gene_id = t2.bio_marker_id"""
            qrysyn = """ select distinct gene_synonym
                                 from ${geneSynonymTable}
			         where tax_id=? and gene_symbol=?""" 
        }
                
        log.info("Start loading Entrez GeneInfo annotation from ${geneInfoTable} to SEARCH_KEYWORD ...")

        selectedOrganism.each{taxonomyId, organism ->

            biomart.eachRow(qry,[taxonomyId])
            {
                long bioMarkerId = it.bio_marker_id
                // Check if it exists with GENE: prefix and insert into SEARCH_KEYWORD if not
                searchKeyword.insertSearchKeyword(it.gene_symbol, bioMarkerId,
                                                  'GENE:'+it.gene_id,
                                                  'Entrez', 'GENE', 'Gene')
                // Determine the id of the keyword that was just inserted
                long searchKeywordID = searchKeyword.getSearchKeywordId(it.gene_symbol, 'GENE')
                // Insert into SEARCH_KEYWORD_TERM
                // check if exists and insert if not:
                if(searchKeywordID){
                    searchKeywordTerm.insertSearchKeywordTerm(it.gene_symbol, searchKeywordID, 1)
                    biomart.eachRow(qrysyn,[taxonomyId, it.gene_symbol]) 
                    {
                        searchKeywordTerm.insertSearchKeywordTerm(it.gene_synonym, searchKeywordID, 2)
                    }
                }
            }
                        

            log.info("End loading Entrex GeneInfo annotation from ${geneInfoTable} to SEARCH_KEYWORD ...")
        }
    }


	void setGeneInfoTable(String geneInfoTable){
		this.geneInfoTable = geneInfoTable
	}


	void setGeneSynonymTable(String geneSynonymTable){
		this.geneSynonymTable = geneSynonymTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}


/*
 * 
 insert into bio_marker nologging 
 (BIO_MARKER_NAME, BIO_MARKER_DESCRIPTION, ORGANISM, PRIMARY_EXTERNAL_ID, BIO_MARKER_TYPE)
 select GENE_SYMBOL, GENE_DESCR, 'HOMO SAPIENS', GENE_ID, 'GENE' 
 from gene_info
 where tax_id=9606 and to_char(gene_id) not in 
 (select PRIMARY_EXTERNAL_ID from bio_marker where ORGANISM = 'HOMO SAPIENS')
 ;
 commit;
 insert into bio_marker nologging 
 (BIO_MARKER_NAME, BIO_MARKER_DESCRIPTION, ORGANISM, PRIMARY_EXTERNAL_ID, BIO_MARKER_TYPE)
 select GENE_SYMBOL, GENE_DESCR, 'RATTUS NORVEGICUS', GENE_ID, 'GENE' 
 from gene_info
 where tax_id=10116 and to_char(gene_id) not in 
 (select PRIMARY_EXTERNAL_ID from bio_marker where ORGANISM = 'RATTUS NORVEGICUS')
 ;
 commit;
 insert into bio_marker nologging 
 (BIO_MARKER_NAME, BIO_MARKER_DESCRIPTION, ORGANISM, PRIMARY_EXTERNAL_ID, BIO_MARKER_TYPE)
 select GENE_SYMBOL, GENE_DESCR, 'MUS MUSCULUS', GENE_ID, 'GENE' 
 from gene_info
 where tax_id=10090 and to_char(gene_id) not in 
 (select PRIMARY_EXTERNAL_ID from bio_marker where ORGANISM = 'MUS MUSCULUS')
 ;
 commit;
 ===========================================================================
 gene_info                                       recalculated daily
 ---------------------------------------------------------------------------
 tab-delimited
 one line per GeneID
 Column header line is the first line in the file.
 Note: subsets of gene_info are available in the DATA/GENE_INFO
 directory (described later)
 ---------------------------------------------------------------------------
 tax_id:
 the unique identifier provided by NCBI Taxonomy
 for the species or strain/isolate
 GeneID:
 the unique identifier for a gene
 ASN1:  geneid
 Symbol:
 the default symbol for the gene
 ASN1:  gene->locus
 LocusTag:
 the LocusTag value
 ASN1:  gene->locus-tag
 Synonyms:
 bar-delimited set of unofficial symbols for the gene
 dbXrefs:
 bar-delimited set of identifiers in other databases
 for this gene.  The unit of the set is database:value.
 chromosome:
 the chromosome on which this gene is placed.
 for mitochondrial genomes, the value 'MT' is used.
 map location:
 the map location for this gene
 description:
 a descriptive name for this gene
 type of gene:
 the type assigned to the gene according to the list of options
 provided in http://www.ncbi.nlm.nih.gov/IEB/ToolBox/CPP_DOC/lxr/source/src/objects/entrezgene/entrezgene.asn
 Symbol from nomenclature authority:
 when not '-', indicates that this symbol is from a
 a nomenclature authority
 Full name from nomenclature authority:
 when not '-', indicates that this full name is from a
 a nomenclature authority
 Nomenclature status:
 when not '-', indicates the status of the name from the
 nomenclature authority (O for official, I for interim)
 Other designations:
 pipe-delimited set of some alternate descriptions that
 have been assigned to a GeneID
 '-' indicates none is being reported.
 Modification date:
 the last date a gene record was updated, in YYYYMMDD format
 */			
