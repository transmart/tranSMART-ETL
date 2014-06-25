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
  

package org.transmartproject.pipeline.pathway

import java.io.File;

import org.transmartproject.pipeline.transmart.BioDataCorrelDescr
import org.transmartproject.pipeline.transmart.BioDataCorrelation
import org.transmartproject.pipeline.transmart.BioMarker
import org.transmartproject.pipeline.transmart.Pathway;
import org.transmartproject.pipeline.transmart.PathwayGene;
import org.transmartproject.pipeline.transmart.SearchKeyword
import org.transmartproject.pipeline.transmart.SearchKeywordTerm
import org.transmartproject.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

class KEGG {

	private static final Logger log = Logger.getLogger(KEGG)
	private static Properties props

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		log.info("Start loading property file ...")
		props = Util.loadConfiguration("conf/Pathway.properties");

                log.info("Loaded props ${props}")
		Sql i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
		Sql i2b2metadata = Util.createSqlFromPropertyFile(props, "i2b2metadata")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

		KEGG kegg = new KEGG()
		File input = new File(props.get("kegg_source"))
		File keggData = new File(props.get("kegg_data_output"))
		kegg.readPathwayData(input, keggData)
		kegg.loadPathwayData(deapp, keggData, props.get("kegg_data_table"))

		File keggDir = new File(props.get("kegg_dir"))
		File keggDef = new File(props.get("kegg_def_output"))
		kegg.readPathwayDefinition(keggDir, keggDef)
		kegg.loadPathwayDefinition(deapp, keggDef, props.get("kegg_def_table"))

		// populate DE_PATHWAY
		kegg.loadPathway(deapp, keggDef, props)

		// populate DE_PATHWAY_GENE
		kegg.loadPathwayGene(deapp, keggData, props)

		// populate BIO_MARKER
		kegg.loadBioMarker(biomart, keggData, keggDef, props)

		// populate BIO_DATA_CORREL_DESCR
		long bioDataCorrelDescrId = kegg.loadBioDataCorrelDescr(biomart)

		// populate BIO_DATA_CORRELATION
		//kegg.loadBioDataCorrelation(biomart, keggData, bioDataCorrelDescrId, props)
		kegg.loadBioDataCorrelation(biomart, deapp, props.get("kegg_data_table"), bioDataCorrelDescrId, props)

		// populate SEARCH_KEYWORD
		kegg.loadSearchKeyword(searchapp, biomart, props)

		// populate SEARCH_KEYWORD_TERM
		//kegg.loadSearchKeywordTerm(searchapp, biomart, props)
	}


	void loadSearchKeyword(Sql searchapp, Sql biomart, Properties props){
		SearchKeyword sk = new SearchKeyword()
		sk.setSearchapp(searchapp)
		sk.setBiomart(biomart)
		if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD table ..."
		}else{
			log.info "Start loading new pathway records into SEARCH_KEYWORD table ..."
			sk.loadPathwaySearchKeyword("KEGG")
			sk.loadGeneSearchKeyword()
			log.info "End loading new pathway records into SEARCH_KEYWORD table ..."
		}
                sk.closeSearchKeyword()
	}

	
    void loadBioDataCorrelation(Sql biomart, Sql deapp, String keggDataTable, long bioDataCorrelDescrId, Properties props){

            Boolean isPostgres = Util.isPostgres()

            if(props.get("skip_bio_data_correlation").toString().toLowerCase().equals("yes")){
                log.info "Skip loading new records into BIO_DATA_CORRELATION table ..."
            }else{
                String qry;
                String qrykegg;

                log.info "Start loading new records into BIO_DATA_CORRELATION table ..."

                if(isPostgres){

                    qrykegg = """select pathway, gene_id, gene from deapp.${keggDataTable}"""
                    qry = """insert into bio_data_correlation(bio_data_id, asso_bio_data_id, bio_data_correl_descr_id)
					select distinct path.bio_marker_id, gene.bio_marker_id, bdcd.bio_data_correl_descr_id
					from bio_marker path, bio_marker gene, bio_data_correl_descr bdcd
					where path.bio_marker_type = 'PATHWAY'
						 and gene.bio_marker_type = 'GENE'
						 and path.primary_external_id = ?
						 and gene.primary_external_id = ?
						 and bdcd.correlation='PATHWAY GENE'
					except
					select bio_data_id, asso_bio_data_id, bio_data_correl_descr_id
					from bio_data_correlation
			 """
                } else {
                    qrykegg = """select pathway, gene_id, gene from deapp.${keggDataTable}"""
                    qry = """insert into bio_data_correlation(bio_data_id, asso_bio_data_id, bio_data_correl_descr_id)
					select distinct path.bio_marker_id, gene.bio_marker_id, bdcd.bio_data_correl_descr_id
					from bio_marker path, bio_marker gene, bio_data_correl_descr bdcd
					where path.bio_marker_type = 'PATHWAY'
						 and gene.bio_marker_type = 'GENE'
						 and path.primary_external_id = ?
						 and gene.primary_external_id = ?
						 and bdcd.correlation='PATHWAY GENE'
					minus
					select bio_data_id, asso_bio_data_id, bio_data_correl_descr_id
					from bio_data_correlation
			 """
                }
                deapp.eachRow(qrykegg) 
                {
                    log.info "load bio_data_correlation for pathway ${it.pathway} gene ${it.gene}"
                    biomart.execute(qry, it.pathway, it.gene_id)
                }
                

                log.info "End loading new records into BIO_DATA_CORRELATION table ..."
            }
	}


	void loadBioDataCorrelation(Sql biomart, File keggData, long bioDataCorrelDescrId, Properties props){
		BioDataCorrelation bdc = new BioDataCorrelation()
		bdc.setBiomart(biomart)
		bdc.setOrganism("HOMO SAPIENS")
		bdc.setSource("KEGG")
		bdc.setBioDataCorrelDescrId(bioDataCorrelDescrId)
		if(props.get("skip_bio_data_correlation").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_DATA_CORRELATION table ..."
		}else{
			log.info "Start loading new records into BIO_DATA_CORRELATION table ..."
			bdc.loadBioDataCorrelation(keggData)
			log.info "End loading new records into BIO_DATA_CORRELATION table ..."
		}
	}


	long loadBioDataCorrelDescr(Sql biomart){
		BioDataCorrelDescr bdcd = new BioDataCorrelDescr()
		bdcd.setBiomart(biomart)
		bdcd.insertBioDataCorrelDescr("PATHWAY GENE", "PATHWAY GENE", "PATHWAY")
		return  bdcd.getBioDataCorrelId("PATHWAY GENE", "PATHWAY")
	}


	void loadBioMarker(Sql biomart, File keggData, File keggDef, Properties props){
		BioMarker bm = new BioMarker()
		bm.setOrganism("HOMO SAPIENS")
		bm.setBiomart(biomart)
		if(props.get("skip_bio_marker").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_MARKER table ..."
		}else{
			bm.loadGenes(keggData)
			bm.loadPathways(keggDef, "KEGG")
		}
	}


	void loadPathwayGene(Sql deapp, File keggData, Properties props){
		PathwayGene pg = new PathwayGene()
		pg.setSource("KEGG")
		pg.setDeapp(deapp)
		if(props.get("skip_de_pathway_gene").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into DE_PATHWAY_GENE table ..."
		}else{
			log.info "Start loading new records into DE_PATHWAY_GENE table ..."
			pg.loadPathwayGene(deapp, keggData)
			log.info "Start loading new records into DE_PATHWAY_GENE table ..."
		}
	}


	void loadPathway(Sql deapp, File keggDef, Properties props){
		Pathway p = new Pathway()
		p.setSource("KEGG")
		p.setDeapp(deapp)
		if(props.get("skip_de_pathway").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into DE_PATHWAY table ..."
		}else{
			log.info "Start loading new records into DE_PATHWAY table ..."
			p.loadPathwayDefinition(keggDef)
			log.info "Stop loading new records into DE_PATHWAY table ..."
		}
	}


    void loadSearchKeywordTerm(Sql searchapp, Sql biomart, Properties props){
		SearchKeywordTerm skt = new SearchKeywordTerm()
		skt.setSearchapp(searchapp)
		skt.setBiomart(biomart)
		if(props.get("skip_search_keyword_term").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD_TERM table ..."
		}else{
			skt.loadSearchKeywordTerm()
		}
                skt.closeSearchKeywordterm()
	}


	/**
	 *  extract KEGG pathway data from hsa.list file
	 *  
	 * @param input		hsa.list
	 * @param output
	 */
	void readPathwayData(File input, File output){
		String [] str
		String pathway, geneId, geneSymbol
		StringBuffer sb = new StringBuffer()

		input.eachLine {
			str = it.split("\t")
			if(str.size() > 2 ){
				pathway = str[0].replace("path:", "")
				geneId = str[1].replace("hsa:", "")
				geneSymbol = str[2].replace("hsa:", "").split(" +")[0]
				String line = pathway + "\t" + geneId + "\t" + geneSymbol
				//log.info line
				sb.append(line + "\n")
			}
		}

		if(output.size() >0){
			output.delete()
			output.createNewFile()
		}
		output.append(sb.toString())
	}


	/**
	 *  extract KEGG pathway definition from *.conf files
	 *  
	 * @param input		the directory stored KEGG's *.conf files
	 * @param output
	 */
	void readPathwayDefinition(File input, File output){
		String [] str = [], str1 =[]
		StringBuffer sb = new StringBuffer()

		input.eachFile {
			if(it.toString().indexOf(".conf") != -1) {
				//log.info it
				it.eachLine { line ->
					if((line.indexOf("hsa:") == -1) && (line.indexOf("?hsa") != -1)){
						str = line.split("\t")
						str1 = str[2].split(": ")
						sb.append(str1[0] + "\t" + str1[1] + "\n")
						//log.info str1[0] + "\t" + str1[1]
					}
				}
			}
		}
		// cannot extract these from *.conf file and must be manually added
		sb.append("hsa01100" + "\t" + "Metabolic pathways\n")
		sb.append("hsa05131" + "\t" + "Shigellosis\n")
		sb.append("hsa05200" + "\t" + "Pathways in cancer\n")
		sb.append("hsa03450" + "\t" + "Non-homologous end-joining\n")
		sb.append("hsa04725" + "\t" + "Cholinergic synapse\n")

		if(output.size() >0){
			output.delete()
			output.createNewFile()
		}
		output.append(sb.toString())
	}


	void loadPathwayData(Sql deapp, File keggData, String KEGGDataTable){

            Boolean isPostgres = Util.isPostgres()
            String qry;

            createKEGGDataTable(deapp, KEGGDataTable)

            if(isPostgres){
                qry = "insert into $KEGGDataTable (pathway, gene_id, gene) values(?, ?, ?)"
            } else {
                qry = "insert into $KEGGDataTable (pathway, gene_id, gene) values(?, ?, ?)"
            }

            if(keggData.size() > 0){
                log.info("Start loading KEGG data file: ${keggData} into ${KEGGDataTable} ...")

                deapp.withTransaction {
                    deapp.withBatch(1000, qry, {stmt ->
                        keggData.eachLine {
                            String [] str = it.split("\t")
                            stmt.addBatch([str[0], str[1], str[2]])
                        }
                                    })
                }
            }else{
                log.error("File ${keggData} is empty ...")
            }
	}


	void loadPathwayDefinition(Sql deapp, File keggDef, String KEGGDefTable){

            createKEGGDefTable(deapp, KEGGDefTable)

            Boolean isPostgres = Util.isPostgres()
            String qry;

            if(isPostgres){
                qry = "insert into $KEGGDefTable (pathway, descr) values(?, ?)"
            } else {
                qry = "insert into $KEGGDefTable (pathway, descr) values(?, ?)"
            }
            
            if(keggDef.size() > 0){
                log.info("Start loading KEGG definition file: ${keggDef} into ${KEGGDefTable} ...")

                deapp.withTransaction {
                    deapp.withBatch(1000, qry, {stmt ->
                        keggDef.eachLine {
                            String [] str = it.split("\t")
                            stmt.addBatch([str[0], str[1]])
                        }
                                    })
                }
            }else{
                log.error("File ${keggDef} is empty ...")
            }
	}


	void createKEGGDataTable(Sql deapp, String KEGGDataTable){

            Boolean isPostgres = Util.isPostgres()
            String qry;
            String qry1;
            String qry2;
//            String qry3;

		log.info "Start creating table: ${KEGGDataTable}"

                if(isPostgres){
                    qry = """ create table ${KEGGDataTable} (
							pathway  varchar(100),
							gene_id  varchar(20),
							gene	 varchar(200)
					 )
			"""
                    qry1 = "select count(*) from pg_tables where tablename=?"
                    qry2 = "drop table ${KEGGDataTable}"
//                    qry3 = "grant select on table ${KEGGDataTable} to biomart"
                } else {
                    qry = """ create table ${KEGGDataTable} (
							pathway  varchar2(100),
							gene_id  varchar2(20),
							gene	 varchar2(200)
					 )
			"""

                    qry1 = "select count(*) from user_tables where table_name=?"
                    qry2 = "drop table ${KEGGDataTable} purge"
//                    qry3 = "grant select on table ${KEGGDataTable} to biomart"
                }

		if((isPostgres && (deapp.firstRow(qry1, [KEGGDataTable])[0] > 0)) ||
                   (deapp.firstRow(qry1, [KEGGDataTable.toUpperCase()])[0] > 0)){
			deapp.execute(qry2)
		}

		deapp.execute(qry)

//                if(isPostgres)
//{
//    log.info ("access '${qry3}'")
//    deapp.execute(qry3);
//}

		log.info "End creating table: ${KEGGDataTable}"
	}


	void createKEGGDefTable(Sql deapp, String KEGGDefTable) {

            Boolean isPostgres = Util.isPostgres()
            String qry;
            String qry1;
            String qry2;

            log.info "Start creating table: ${KEGGDefTable}"

            if(isPostgres){
                qry = """ create table ${KEGGDefTable} (
							pathway  varchar(100),
							descr	 varchar(500)
				) """
		qry1 = "select count(*) from pg_tables where tablename=?"
                qry2 = "drop table ${KEGGDefTable}"
            } else {
                qry = """ create table ${KEGGDefTable} (
							pathway  varchar2(100),
							descr	 varchar2(500)
				) """
		qry1 = "select count(*) from user_tables where table_name=?"
                qry2 = "drop table ${KEGGDefTable} purge"
            }

            if((isPostgres && deapp.firstRow(qry1, [KEGGDefTable])[0] > 0) ||
               (deapp.firstRow(qry1, [KEGGDefTable.toUpperCase()])[0] > 0)) {
                deapp.execute(qry2)
            }

            deapp.execute(qry)

            log.info "End creating table: ${KEGGDefTable}"
	}

}
