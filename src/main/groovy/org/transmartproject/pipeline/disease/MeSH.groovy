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
  

package org.transmartproject.pipeline.disease

import org.transmartproject.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator;
import org.transmartproject.pipeline.transmart.BioDataUid
import org.transmartproject.pipeline.transmart.SearchKeyword
import org.transmartproject.pipeline.transmart.SearchKeywordTerm

class MeSH {

	private static final Logger log = Logger.getLogger(MeSH)
	private static Properties props
	
        private static SearchKeyword searchKeyword
        private static SearchKeywordTerm searchKeywordTerm

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");
		MeSH mesh = new MeSH()
		
		if(args.size() > 0){
			log.info("Start loading property files conf/Common.properties and ${args[0]} ...")
			mesh.setProperties(Util.loadConfiguration(args[0]));
		} else {
			log.info("Start loading property files conf/Common.properties and conf/MeSH.properties ...")
			mesh.setProperties(Util.loadConfiguration("conf/MeSH.properties"));
		}

		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

                searchKeyword = new SearchKeyword()
                searchKeyword.setSearchapp(searchapp)

                searchKeywordTerm = new SearchKeywordTerm()
                searchKeywordTerm.setSearchapp(searchapp)

		// create a temporary table for MeSH data
		if(props.get("skip_mesh_table").toString().toLowerCase().equals("yes")){
			log.info "Skip creating temporary tables for MeSH data ..."
		}else{
			log.info "Start creating temporary tables for MeSH data ..."
			mesh.createMeSHTable(biomart, props.get("mesh_table"))
			mesh.createMeSHSynonymTable(biomart, props.get("mesh_synonym_table"))
		}

		File input = new File(props.get("mesh_source"))
		File output = new File(input.getParent() + "/MeSH.tsv")
		File entry = new File(input.getParent() + "/MeSH_Entry.tsv")

		String meshTree = props.get("load_mesh_tree_node")


		mesh.readMeSH(input, output, entry, meshTree)
		mesh.loadMeSH(biomart, output, props.get("mesh_table"))
		mesh.loadMeSHSynonym(biomart, entry, props.get("mesh_synonym_table"))
		mesh.loadBioDisease(biomart)
		mesh.loadBioDataExtCode(biomart)
		mesh.loadBioDataUid(biomart)
		mesh.loadSearchKeyword(searchapp,biomart)
                searchKeyword.closeSearchKeyword()
                searchKeywordTerm.closeSearchKeywordTerm()

                print new Date()
                println" MeSH diseases load completed successfully"
	}


	void readMeSH(File input, File output, File synonym, String meshTree){

		if(input.size() > 0){
			log.info("Start processing MeSH file: ${input} ...")

			StringBuffer sb = new StringBuffer()
			StringBuffer entry = new StringBuffer()
			String mh = "", ui = "", mn = "", entry1 = "", entry2 = ""
			String [] treeNodes
			boolean isNeeded = false

			input.eachLine{
				if(it.indexOf("*NEWRECORD") == 0){
					mh = ""
					ui = ""
					mn = ""
				}

				else if(it.indexOf("MH ") == 0) mh = it.split("=")[1].trim()

				else if(it.indexOf("MN ") == 0) {
                                    mn = it.split("=")[1].trim()

                                    if(meshTree.equals(null) || meshTree.equals("")) { // read all entries
					isNeeded = true
                                    }
                                    else if(meshTree.indexOf(",")){ // read any with prefix in load_mesh_tree_node
					treeNodes = meshTree.split(",")
					treeNodes.each{ tree ->
                                            if(it.indexOf("MN = $tree") == 0) isNeeded = true
					}
                                    }else{ // one prefix in load_mesh_tree_node
					if(it.indexOf("MN = $meshTree") == 0) isNeeded = true
                                    }
                                }
                                
				else if(it.indexOf("UI ") == 0) {
					ui = it.split("=")[1].trim()
					if(isNeeded && (mh.size() > 0) && (ui.size() >0)) sb.append("$ui\t$mh\t$mn\n")
					isNeeded = false
				}

				else if(it.indexOf("ENTRY") == 0) {
					entry1 = it.split("=")[1].trim()
					if(entry1.indexOf("|") != -1) {
						entry2 = entry1.split("\\|")[0].trim()
						entry.append(mh + "\t" + entry2 + "\n")
					}else{
						entry.append(mh + "\t" + entry1 + "\n")
					}
				}

				else if(it.indexOf("PRINT ENTRY") == 0) {
					entry1 = it.split("=")[1].trim()
					if(entry1.indexOf("|") != -1) {
						entry2 = entry1.split("\\|")[0].trim()
						entry.append(mh + "\t" + entry2 + "\n")
					}else{
						entry.append(mh + "\t" + entry1 + "\n")
					}
				}
			}

			if(output.size() > 0){
				output.delete()
				output.createNewFile()
			}
			output.append(sb.toString())

			if(synonym.size() > 0){
				synonym.delete()
				synonym.createNewFile()
			}
			synonym.append(entry.toString())
		}else{
			log.error("File ${input} is empty ...")
		}
	}



	void loadBioDisease(Sql biomart){

		String MeSHTable = props.get("mesh_table")

		if(props.get("skip_bio_disease").toString().toLowerCase().equals("yes")){
			log.info("Skip loading MeSH data from ${MeSHTable} to BIO_DISEASE ...")
		}else{
			log.info("Start loading MeSH data from ${MeSHTable} to BIO_DISEASE ...")

			String qry = """ insert into bio_disease (disease, mesh_code, prefered_name) 
						     select mh, ui, mh from $MeSHTable
						     where ui not in (select mesh_code from bio_disease where mesh_code is not null)"""
			biomart.execute(qry)

			log.info("End loading MeSH data from ${MeSHTable} to BIO_DISEASE ...")
		}
	}


    void loadBioDataExtCode(Sql biomart){
        Boolean isPostgres = Util.isPostgres()
        String qry;
        String MeSHSynonymTable = props.get("mesh_synonym_table")

        if(isPostgres){
            qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
							 select d.bio_disease_id, m.entry, 'Alias', 'SYNONYM', 'BIO_DISEASE' 
                             from bio_disease d, $MeSHSynonymTable m
							 where d.disease::text = m.mh and d.disease is not null
                             except
                             select bio_data_id, code, 'Alias', 'SYNONYM', 'BIO_DISEASE'
                             from bio_data_ext_code"""
        } else {
            qry = """ insert into bio_data_ext_code(bio_data_id, code, code_source, code_type, bio_data_type)
							 select d.bio_disease_id, m.entry, 'Alias', 'SYNONYM', 'BIO_DISEASE' 
                             from bio_disease d, $MeSHSynonymTable m
							 where to_char(d.disease) = m.mh and d.disease is not null
                             minus
                             select bio_data_id, code, 'Alias', 'SYNONYM', 'BIO_DISEASE'
                             from bio_data_ext_code"""
        }
        

		if(props.get("skip_bio_data_ext_code").toString().toLowerCase().equals("yes")){
			log.info("Skip loading MeSH data from ${MeSHSynonymTable} to BIO_DATA_EXT_CODE ...")
		}else{
			log.info("Start loading MeSH data from ${MeSHSynonymTable} to BIO_DATA_EXT_CODE ...")

			biomart.execute(qry)

			log.info("End loading MeSH data from ${MeSHSynonymTable} to BIO_DATA_EXT_CODE ...")
		}
	}


	void loadBioDataUid(Properties props, Sql biomart){

            BioDataUid bdu = new BioDataUid()
            bdu.setBiomart(biomart)
            bdu.loadBioDataUid()
	}

    void loadSearchKeyword(Sql searchapp, Sql biomart){
        Boolean isPostgres = Util.isPostgres()
        String qry;
        String qrysyn;
        String MeSHTable = props.get("mesh_table")
        String MeSHSynonymTable = props.get("mesh_synonym_table")

                if(isPostgres) {
                    qry = """ select distinct t1.mh, t2.bio_disease_id, t1.ui
                             from biomart.$MeSHTable t1, biomart.bio_disease t2
						     where t1.ui=t2.mesh_code
				         """
                    qrysyn = """ select distinct t1.mh, t1.entry
                             from biomart.$MeSHSynonymTable t1
						     where t1.mh=?
				         """ 
                } else {
                    qry = """ select distinct t1.mh, t2.bio_disease_id, t1.ui
                             from biomart.$MeSHTable t1, biomart.bio_disease t2
						     where t1.ui=t2.mesh_code
				         """
                    qrysyn = """ select distinct t1.mh, t1.entry
                             from biomart.$MeSHSynonymTable t1
						     where t1.mh=?
				         """ 
		    String oldqry = """ insert into SEARCH_KEYWORD (KEYWORD, BIO_DATA_ID, UNIQUE_ID, DATA_CATEGORY, DISPLAY_DATA_CATEGORY)
					   	     select distinct t1.mh, t2.bio_disease_id, 'DIS:'||t1.ui, 'DISEASE', 'Disease'  
                             from biomart.$MeSHTable t1, biomart.bio_disease t2
						     where t1.ui=to_char(t2.mesh_code)  
                                 and t2.bio_disease_id not in 
                                     (select bio_data_id from search_keyword 
                                      where data_category='DISEASE' and bio_data_id is not null)
				         """
                }
                
		if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
			log.info("Skip loading MeSH data from ${MeSHTable} to SEARCH_KEYWORD ...")

		}else{
			log.info("Start loading MeSH data from ${MeSHTable} to SEARCH_KEYWORD ...")

			biomart.eachRow(qry)
                        {
                            long bioDiseaseId = it.bio_disease_id
                            // Check if it exists with DIS: prefix and insert into SEARCH_KEYWORD if not
                            searchKeyword.insertSearchKeyword(it.mh, bioDiseaseId,
                                                              'DIS:'+it.ui,
                                                              'MeSH', 'DISEASE', 'Disease')
                            // Determine the id of the keyword that was just inserted
                            long searchKeywordID = searchKeyword.getSearchKeywordId(it.mh, 'DISEASE')
                            // Insert into SEARCH_KEYWORD_TERM
                            // check if exists and inserts if not:
                            if(searchKeywordID){
                                searchKeywordTerm.insertSearchKeywordTerm(it.mh, searchKeywordID, 1)
                                biomart.eachRow(qrysyn,[it.mh]) 
                                {
                                    searchKeywordTerm.insertSearchKeywordTerm(it.entry, searchKeywordID, 2)
                                }
                            }
                        }
                        

			log.info("End loading MeSH data from ${MeSHTable} to SEARCH_KEYWORD ...")
		}
	}


	void loadMeSHSynonym(Sql biomart, File meshEntry, String MeSHSynonymTable){

		String qry = "insert into $MeSHSynonymTable (mh, entry) values(?, ?)"

		if(meshEntry.size() > 0){
			log.info("Start loading MeSH synonym file: ${meshEntry} into ${MeSHSynonymTable} ...")

			biomart.withTransaction {
                            biomart.withBatch(1000, qry, {stmt ->
					meshEntry.eachLine {
						String [] str = it.split("\t")
						stmt.addBatch([str[0], str[1]])
					}
				})
			}

		}else{
			log.error("File ${meshEntry} is empty ...")
		}
	}


	void loadMeSH(Sql biomart, File mesh, String MeSHTable){

		String qry = "insert into $MeSHTable (ui, mh, mn) values(?, ?, ?)"

		if(mesh.size() > 0){
			log.info("Start loading MeSH file: ${mesh} into ${MeSHTable} ...")

			biomart.withTransaction {
                            biomart.withBatch(1000, qry, {stmt ->
                                mesh.eachLine {
                                    String [] str = it.split("\t")
                                    stmt.addBatch([str[0], str[1], str[2]])
                                }
                            })
			}

		}else{
			log.error("File ${mesh} is empty ...")
		}
	}

	void createMeSHTable(Sql biomart, String MeSHTable){

            Boolean isPostgres = Util.isPostgres()

            log.info "Start creating MeSH table: ${MeSHTable}"

            String qry;
            String qry1;
            String qry2;
            String qrygrant;

            if(isPostgres){
                qry = """ create table ${MeSHTable} (
						ui  character varying(20) primary key,
						mh	character varying(200),
						mn	character varying(200)
					 )
			  """
                qry1 = "select count(1) from pg_tables where tablename=?"
                qry2 = "truncate table ${MeSHTable}"
                qrygrant = "grant select on ${MeSHTable} to searchapp"

                if(biomart.firstRow(qry1, [MeSHTable])[0] > 0) {
                    log.info "Truncating table ${MeSHTable}"
                    biomart.execute(qry2)
                }
                else{
                    log.info "Creating table ${MeSHTable}"
                    biomart.execute(qry)
                }

                biomart.execute(qrygrant)

            } else {
                qry = """ create table ${MeSHTable} (
						UI  varchar2(20) primary key,
						MH	varchar2(200),
						MN	varchar2(200)
					 )
			 """
                qry1 = "select count(1) from user_tables where table_name=?"
                qry2 = "truncate table ${MeSHTable}"
                if(biomart.firstRow(qry1, [MeSHTable.toUpperCase()])[0] > 0) {
                    log.info "Truncating table ${MeSHTable}"
                    biomart.execute(qry2)
                }
                else{
                    log.info "Creating table ${MeSHTable}"
                    biomart.execute(qry)
                }
            }
                
            log.info "End creating table: ${MeSHTable}"
	}


	void createMeSHSynonymTable(Sql biomart, String MeSHSynonymTable){

            Boolean isPostgres = Util.isPostgres()

            log.info "Start creating table: ${MeSHSynonymTable}"

            String qry = "";
            String qry1 = "";
            String qry2 = "";
            String qrygrant = "";

            if(isPostgres) {
                qry = """ create table ${MeSHSynonymTable} (
							MH      character varying(200),
							ENTRY	character varying(200)
						 )
					"""
                qry1 = "select count(1) from pg_tables where tablename=?"
                qry2 = "drop table ${MeSHSynonymTable}"
                qrygrant = "grant select on table ${MeSHSynonymTable} to searchapp"
		if(biomart.firstRow(qry1, [MeSHSynonymTable])[0] > 0){
                    log.info "Dropping table ${MeSHSynonymTable} postgres"
                    biomart.execute(qry2)
		}

		biomart.execute(qry)

                biomart.execute(qrygrant)

            } else {
                qry = """ create table ${MeSHSynonymTable} (
							MH      varchar2(200),
							ENTRY	varchar2(200)
						 )
					"""
                qry1 = "select count(1) from user_tables where table_name=?"
                qry2 = "drop table ${MeSHSynonymTable} purge"

                if(biomart.firstRow(qry1, [MeSHSynonymTable.toUpperCase()])[0] > 0){
                    log.info "Dropping table ${MeSHSynonymTable} postgres"
                    biomart.execute(qry2)
                }

                biomart.execute(qry)
            }
                                   
            log.info "End creating table: ${MeSHSynonymTable}"
	}

	
	void setProperties(Properties props){
		this.props = props
	}

}
