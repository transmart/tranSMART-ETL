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

class GeneOntology {

	private static final Logger log = Logger.getLogger(GeneOntology)
	private static Properties props

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		log.info("Start loading property file ...")
		props = Util.loadConfiguration("conf/Pathway.properties");

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql biomartuser = Util.createSqlFromPropertyFile(props, "biomart_user")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

		println new Date()
		
		GeneOntology geneOntology = new GeneOntology()

		geneOntology.createPathwayTable(biomartuser, props.get("pathway_table"))
		geneOntology.createPathwayDataTable(biomartuser, props.get("pathway_data_table"))


		//Map humanGeneId = [:, mouseGeneId = [:], ratGeneId = [:]

		// Homo sapiens
		//humanGeneId = geneOntology.getGeneId(biomart, "9606", props.get("gene_info_table"))

		// Mus musculus
		//mouseGeneId = geneOntology.getGeneId(biomart, "10090", props.get("gene_info_table"))

		// Rattus norvegicus
		//ratGeneId = geneOntology.getGeneId(biomart, "10116", props.get("gene_info_table"))

		//println humanGeneId.size() + "\t" + mouseGeneId.size() + "\t" + ratGeneId.size()

		// process Gene Ontology data
		File goInput = new File(props.get("gene_ontology_source"))
		File goOutput = new File(goInput.getParent() + "/gene_ontology.tsv")
		geneOntology.readGeneOntology(goInput, goOutput)
		geneOntology.loadGeneOntology(biomartuser, goOutput, props.get("pathway_table"))

		println new Date()
		

		// process Gene Association data for Homo sapiens
		File goahInput = new File(props.get("gene_association_human"))
		File goahOutput = new File(goahInput.getParent() + "/goa_human.tsv")
		File goahReject = new File(goahInput.getParent() + "/goa_human_reject.tsv")
		//geneOntology.readGeneAssociation(goahInput, goahOutput, goahReject, humanGeneId, "Homo sapiens")
		//geneOntology.loadGeneAssociation(biomart, goahOutput, props.get("pathway_data_table"))

		geneOntology.readGeneAssociation(goahInput, goahOutput)
		geneOntology.loadGeneAssociation(biomartuser, goahOutput, props.get("pathway_data_table"), "Homo sapiens")

		println new Date()
		
		// process Gene Association data for Mus musculus
		File goamInput = new File(props.get("gene_association_mouse"))
		File goamOutput = new File(goamInput.getParent() + "/goa_mgi.tsv")
		File goamReject = new File(goamInput.getParent() + "/goa_mgi_reject.tsv")
		//geneOntology.readGeneAssociation(goamInput, goamOutput, goamReject, mouseGeneId, "Mus musculus")
		//geneOntology.loadGeneAssociation(biomart, goamOutput, props.get("pathway_data_table"))

		geneOntology.readGeneAssociation(goamInput, goamOutput)
		geneOntology.loadGeneAssociation(biomartuser, goamOutput, props.get("pathway_data_table"), "Mus musculus")

		println new Date()
		
		// process Gene Association data for Rattus norvegicus
		File goarInput = new File(props.get("gene_association_rat"))
		File goarOutput = new File(goarInput.getParent() + "/goa_rgd.tsv")
		File goarReject = new File(goarInput.getParent() + "/goa_rgd_reject.tsv")
		//geneOntology.readGeneAssociation(goarInput, goarOutput, goarReject, ratGeneId, "Rattus norvegicus")
		//geneOntology.loadGeneAssociation(biomart, goarOutput, props.get("pathway_data_table"))

		geneOntology.readGeneAssociation(goarInput, goarOutput)
		geneOntology.loadGeneAssociation(biomartuser, goarOutput, props.get("pathway_data_table"), "Rattus norvegicus")

		println new Date()
		
		// create indexes 
		geneOntology.createIndex(biomartuser, props.get("pathway_data_table"))
		
		println new Date()
		
		// populate BIO_MARKER
		if(props.get("skip_bio_marker").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_MARKER ..."
		}else{
                    geneOntology.loadBioMarker(biomartuser, biomart,
                                               props.get("pathway_table"), props.get("pathway_data_table"))
			
		}

                println new Date()

		// populate DE_PATHWAY
		if(props.get("skip_de_pathway").toString().toLowerCase().equals("yes")){
			log.info "Skip loading records into DE_PATHWAY ..."
		}else{
			Pathway p = new Pathway()
			p.setSource("GO")
			p.setDeapp(deapp)
			p.setBiomartuser(biomartuser)
			//p.loadPathway(goOutput)
			p.loadPathway(props.get("pathway_table"), props.get("pathway_data_table"))
		}

		println new Date()
		
		Map pathwayId = geneOntology.getPathwayId(deapp, "GO")

		println new Date()
		
		// populate DE_PATHWAY_GENE
		if(props.get("skip_de_pathway_gene").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into DE_PATHWAY_GENE ..."
		}else{
			PathwayGene pg = new PathwayGene()
			pg.setSource("GO")
			pg.setDeapp(deapp)
			pg.setBiomart(biomart)
			pg.setBiomartuser(biomartuser)

			// load de_pathway_gene for human
			log.info "Start loading DE_PATHWAY_GENE for GO  ..."
			//pg.loadPathwayGene(goahOutput, humanGeneId, pathwayId)
			pg.loadPathwayGene(props.get("pathway_data_table"))

			// load de_pathway_gene for Mus musculus
			//log.info "Start loading DE_PATHWAY_GENE for GO Mouse ..."
			//pg.loadPathwayGene(goamOutput, mouseGeneId, pathwayId)

			// load de_pathway_gene for Rattus norvegicus
			//log.info "Start loading DE_PATHWAY_GENE for GO Rat ..."
			//pg.loadPathwayGene(goarOutput, ratGeneId, pathwayId)
		}

		println new Date()
		

		// populate BIO_DATA_CORREL_DESCR
		BioDataCorrelDescr bdcd = new BioDataCorrelDescr()
		bdcd.setBiomart(biomart)
		bdcd.insertBioDataCorrelDescr("PATHWAY GENE", "PATHWAY GENE", "PATHWAY")
		long bioDataCorrelDescrId = bdcd.getBioDataCorrelId("PATHWAY GENE", "PATHWAY")
		
		println new Date()
		

		// populate BIO_DATA_CORRELATION
		if(props.get("skip_bio_data_correlation").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_DATA_CORRELATION ..."
		}else{
			BioDataCorrelation bdc = new BioDataCorrelation()
			bdc.setBiomart(biomartuser)
			bdc.setSource("GO")
			bdc.setBioDataCorrelDescrId(bioDataCorrelDescrId)

			bdc.loadBioDataCorrelation(props.get("pathway_data_table"))

			/*
			 bdc.setOrganism("Homo sapiens")
			 bdc.loadBioDataCorrelation(goahOutput, humanGeneId)
			 bdc.setOrganism("Mus musculus")
			 bdc.loadBioDataCorrelation(goamOutput, mouseGeneId)
			 bdc.setOrganism("Rattus norvegicus")
			 bdc.loadBioDataCorrelation(goarOutput, ratGeneId)
			 */
		}

		println new Date()
		
		// populate SEARCH_KEYWORD
		if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD ..."
		}else{
                        SearchKeyword sk = new SearchKeyword()

                        sk.setSearchapp(searchapp)
                        sk.setBiomart(biomart)

			sk.loadPathwaySearchKeyword("GO")
			sk.loadGeneSearchKeyword()
                        sk.closeSearchKeyword()
		}

		println new Date()
		
		// populate SEARCH_KEYWORD_TERM
		if(props.get("skip_search_keyword_term").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD_TERM  ..."
		}else{
                        SearchKeywordTerm skt = new SearchKeywordTerm()
			skt.setSearchapp(searchapp)

			skt.loadSearchKeywordTerm()
                        skt.closeSearchKeywordTerm()
		}
		
		println new Date()
		
	}


	void loadBioMarker(Sql biomartuser, Sql biomart, String pathwayTable, String pathwayDataTable){

		log.info "Start loading GO pathway into BIO_MARKER ..."
		
                String qry1 = """ select distinct t1.descr, t1.descr, t2.organism, t2.pathway
						 from gene_ontology t1, gene_ontology_data t2
						 where t1.pathway=t2.pathway
					"""

                BioMarker bioMarker = new BioMarker()
                bioMarker.setBiomart(biomart);
		biomartuser.eachRow(qry1) 
                {
                    bioMarker.setOrganism(it.organism)
                    bioMarker.insertBioMarker(it.descr, it.descr, 'GO', it.pathway, 'PATHWAY')
                }
		
		log.info "End loading GO pathway into BIO_MARKER ..."
	}


	void readGeneOntology(File goInput, File goOutput){

		if(goInput.size() >0){
			log.info ("Start processing " + goInput.toString())
		}else{
			throw new RuntimeException(goInput.toString() + " is empty")
		}

		StringBuffer sb = new StringBuffer()

		boolean termFlag = false
		String goId = "", goName = ""
		goInput.eachLine {

			if(it.indexOf("[Term]") != -1) {
				termFlag = true

				// only keep data for human, mouse and rat
				if((goId.size() > 0) && goName.size() >0) sb.append("$goId\t$goName\n")
				goId = ""
				goName = ""
			}

			if(termFlag && (it.indexOf("id: ") != -1)){
				goId = it.replace("id: ", "").trim()
			}

			if(termFlag && (it.indexOf("name: ") != -1)){
				goName = it.replace("name: ", "").trim()
				termFlag = false
			}
		}

		if(goOutput.size() >0){
			goOutput.delete()
			goOutput.createNewFile()
		}
		goOutput.append(sb.toString())
	}


	void loadGeneOntology(Sql biomartuser, File goOutput, String pathwayTable){

		if(goOutput.size() > 0){
			log.info ("Start loading " + goOutput.toString())
		}else{
			throw new RuntimeException(goOutput.toString() + " is empty")
		}

		String qry = " insert into ${pathwayTable}(pathway, descr) values(?, ?)"

		biomartuser.withTransaction {
			biomartuser.withBatch(qry, {stmt ->
				goOutput.eachLine {
					String [] str = it.split("\t")
					stmt.addBatch([str[0], str[1]])
				}
			}
			)
		}
	}


	void readGeneAssociation(File goaInput, File goaOutput, File goaReject, Map geneList, String organism){

		Map goa = [:]

		if(goaInput.size() >0){
			log.info ("Start processing " + goaInput.toString())
		}else{
			throw new RuntimeException(goaInput.toString() + " is empty")
		}

		String [] str = []
		StringBuffer sb = new StringBuffer()

		goaInput.eachLine {
			if(it.indexOf("!") == -1) {
				str = it.split("\t")
				//sb.append(str[4].trim() + "\t" + str[2].trim() + "\n")
				goa[str[4].trim() + "\t" + str[2].trim()] = 1
			}
		}

		if(goaOutput.size() >0){
			goaOutput.delete()
			goaOutput.createNewFile()
		}

		StringBuffer sbReject = new StringBuffer()
		goa.each{k, v ->
			String [] s = k.split("\t")
			if(geneList[s[1]].equals(null)){
				sbReject.append(k + "\n")
			}else{
				sb.append(s[0] + "\t" + geneList[s[1]] + "\t" + s[1] + "\t" + organism + "\n")
			}
		}
		goaOutput.append(sb.toString())
		goaReject.append(sbReject.toString())
	}


	void readGeneAssociation(File goaInput, File goaOutput){

		Map goa = [:]

		if(goaInput.size() >0){
			log.info ("Start processing " + goaInput.toString())
		}else{
			throw new RuntimeException(goaInput.toString() + " is empty")
		}

		String [] str = []
		StringBuffer sb = new StringBuffer()

		goaInput.eachLine {
			if(it.indexOf("!") == -1) {
				str = it.split("\t")
				goa[str[4].trim() + "\t" + str[2].trim()] = 1
			}
		}

		if(goaOutput.size() >0){
			goaOutput.delete()
			goaOutput.createNewFile()
		}

		goa.each{k, v ->
			sb.append(k + "\n")
		}
		goaOutput.append(sb.toString())
	}


	void loadGeneAssociation(Sql biomartuser, File goaOutput, String pathwayDataTable){

		if(goaOutput.size() > 0){
			log.info ("Start loading " + goaOutput.toString())
		}else{
			throw new RuntimeException(goaOutput.toString() + " is empty")
		}

		String qry = " insert into ${pathwayDataTable}(pathway, gene_id, gene_symbol, organism) values(?, ?, ?, ?)"

		biomartuser.withTransaction {
			biomartuser.withBatch(qry, {stmt ->
				goaOutput.eachLine {
					String [] str = it.split("\t")
					stmt.addBatch([
						str[0],
						str[1],
						str[2],
						str[3]
					])
				}
			}
			) }
	}

	
	void createIndex(Sql biomartuser, String pathwayDataTable){

            Boolean isPostgres = Util.isPostgres()
		
            log.info "Start creating indexes ..."
	
            if(isPostgres) {
		String qry = "create index idx_god_symbol on ${pathwayDataTable}(gene_symbol)"	
		biomartuser.execute(qry)
		
		qry = " create index idx_god_organism on ${pathwayDataTable}(organism)"
		biomartuser.execute(qry)
            }else{
    
		String qry = "create index idx_god_symbol on ${pathwayDataTable} (upper(gene_symbol)) "	
		biomartuser.execute(qry)
		
		qry = " create bitmap index idx_god_organism on ${pathwayDataTable} (upper(organism))"
		biomartuser.execute(qry)
            }
            
            
            log.info "End creating indexes ..."
	}

	
	void loadGeneAssociation(Sql biomart, File goaOutput, String pathwayDataTable, String organism){

		if(goaOutput.size() > 0){
			log.info ("Start loading " + goaOutput.toString())
		}else{
			throw new RuntimeException(goaOutput.toString() + " is empty")
		}

		String qry = " insert into ${pathwayDataTable}(pathway, gene_symbol, organism) values(?, ?, ?)"

		biomart.withTransaction {
			biomart.withBatch(qry, {stmt ->
				goaOutput.eachLine {
					String [] str = it.split("\t")
					stmt.addBatch([
						str[0],
						str[1],
						organism
					])
				}
			}
			) }
	}



	Map getGeneId(Sql biomart, String taxId, String geneInfoTable){

		Map geneId = [:]
		String qry = "select upper(gene_symbol) gene_symbol, gene_id from ${geneInfoTable} where tax_id=?"
		biomart.eachRow(qry, [taxId]) {
			geneId[it.gene_symbol] = it.gene_id
		}
		return geneId
	}


	Map getPathwayId(Sql deapp, String source){

		Map pathwayId = [:]
		String qry = "select externalid, id from de_pathway where source=?"
		deapp.eachRow(qry, [source]) {
			pathwayId[it.externalid] = it.id
		}
		return pathwayId
	}



	void createPathwayTable(Sql biomartuser, String pathwayTable){

            Boolean isPostgres = Util.isPostgres()
            String qry;
            String qry1;
            String qry2;


            log.info "Start creating table: ${pathwayTable}"

            if(isPostgres){
		qry = """ create table ${pathwayTable} (
							pathway  		character varying(100),
							descr			character varying(500)
						 )
					"""

		qry1 = "select count(*)  from pg_tables where tablename=?"
                qry2 = "drop table ${pathwayTable}"
            } else {
		qry = """ create table ${pathwayTable} (
							pathway  		varchar2(100),
							descr			varchar2(500)
						 )
					"""

		qry1 = "select count(*)  from user_tables where table_name=?"
                qry2 = "drop table ${pathwayTable} purge"
            }
            
            if((isPostgres && (biomartuser.firstRow(qry1, [pathwayTable])[0] > 0)) ||
               (biomartuser.firstRow(qry1, [pathwayTable.toUpperCase()])[0] > 0)){
			biomartuser.execute(qry2)
		}

		biomartuser.execute(qry)

		log.info "End creating table: ${pathwayTable}"

	}


	void createPathwayDataTable(Sql biomartuser, String pathwayDataTable){

            Boolean isPostgres = Util.isPostgres()
            String qry;
            String qry1;
            String qry2;

		log.info "Start creating table: ${pathwayDataTable}"

            if(isPostgres){
		qry = """ create table ${pathwayDataTable} (
							pathway  		character varying(100),
							gene_id			character varying(20),
							gene_symbol		character varying(200),
							organism		character varying(100)
						 )
					"""

		qry1 = "select count(*)  from pg_tables where tablename=?"
                qry2 = "drop table ${pathwayDataTable}"
            } else {
		qry = """ create table ${pathwayDataTable} (
							pathway  		varchar2(100),
							gene_id			varchar2(20),
							gene_symbol		varchar2(200),
							organism		varchar2(100)
						 )
					"""

		qry1 = "select count(*)  from user_tables where table_name=?"
                qry2 = "drop table ${pathwayDataTable} purge"
            }
            

            if((isPostgres && (biomartuser.firstRow(qry1, [pathwayDataTable])[0] > 0)) ||
               (biomartuser.firstRow(qry1, [pathwayDataTable.toUpperCase()])[0] > 0)){
			biomartuser.execute(qry2)
		}

		biomartuser.execute(qry)

		log.info "End creating table: ${pathwayDataTable}"

	}

}
