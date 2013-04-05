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
  

package com.recomdata.pipeline.pathway

import com.recomdata.pipeline.transmart.BioDataCorrelDescr
import com.recomdata.pipeline.transmart.BioDataCorrelation
import com.recomdata.pipeline.transmart.BioMarker
import com.recomdata.pipeline.transmart.Pathway;
import com.recomdata.pipeline.transmart.PathwayGene;
import com.recomdata.pipeline.transmart.SearchKeyword
import com.recomdata.pipeline.transmart.SearchKeywordTerm
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

class Ingenuity {

	private static final Logger log = Logger.getLogger(Ingenuity)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		log.info("Start loading property file loader.properties ...")
		Properties props = Util.loadConfiguration("conf/loader.properties");

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql searchapp = Util.createSqlFromPropertyFile(props, "searchapp")

		Ingenuity ingenuity = new Ingenuity()

		File input = new File(props.get("ingenuity_source"))
		File ipaDef = new File(input.getParent() + "/IngenuityDef.tsv")
		File ipaData = new File(input.getParent() + "/IngenuityData.tsv")
		File ipaDataReject = new File(input.getParent() + "/IngenuityDataReject.tsv")

		// 9606 -- Homo sapiens
		Map entrez = ingenuity.getGeneId(biomart, "9606")
		ingenuity.readIngenuity(input, ipaDef, ipaData, ipaDataReject, entrez)
		ingenuity.loadIngenuity(biomart, ipaData, props.get("pathway_data_table"), "Homo sapiens")


		// populate DE_PATHWAY
		if(props.get("skip_de_pathway").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Ingenuity pathway into DE_PATHWAY ..."
		}else{
			Pathway p = new Pathway()
			p.setSource("Ingenuity")
			p.setDeapp(deapp)
			p.loadPathway(ipaDef)
		}


		Map pathwayId = ingenuity.getPathwayId(deapp, "Ingenuity")


		// populate DE_PATHWAY_GENE
		if(props.get("skip_de_pathway_gene").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into DE_PATHWAY_GENE ..."
		}else{
			PathwayGene pg = new PathwayGene()
			pg.setSource("Ingenuity")
			pg.setDeapp(deapp)

			log.info "Start loading DE_PATHWAY_GENE for Ingenuity ..."
			pg.loadPathwayGene(ipaData, pathwayId)
		}

		
		// populate BIO_MARKER
		if(props.get("skip_bio_marker").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_MARKER ..."
		}else{
			BioMarker bm = new BioMarker()
			bm.setBiomart(biomart)
			bm.setOrganism("Homo sapiens")
			//bm.loadGenes(ipaData)
			bm.loadPathways(ipaDef, "Ingenuity")
		}


		// populate BIO_DATA_CORREL_DESCR
		BioDataCorrelDescr bdcd = new BioDataCorrelDescr()
		bdcd.setBiomart(biomart)
		bdcd.insertBioDataCorrelDescr("PATHWAY GENE", "PATHWAY GENE", "PATHWAY")
		long bioDataCorrelDescrId = bdcd.getBioDataCorrelId("PATHWAY GENE", "PATHWAY")


		// populate BIO_DATA_CORRELATION
		if(props.get("skip_bio_data_correlation").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into BIO_DATA_CORRELATION ..."
		}else{
			BioDataCorrelation bdc = new BioDataCorrelation()
			bdc.setBiomart(biomart)
			bdc.setSource("Ingenuity")
			bdc.setBioDataCorrelDescrId(bioDataCorrelDescrId)
			bdc.setOrganism("Homo sapiens")
			//bdc.loadBioDataCorrelation(ipaData)
			bdc.loadBioDataCorrelation(props.get("pathway_data_table"))
		}


		// populate SEARCH_KEYWORD
		if(props.get("skip_search_keyword").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD ..."
		}else{
			SearchKeyword sk = new SearchKeyword()
			sk.setSearchapp(searchapp)
			sk.loadPathwaySearchKeyword()
			sk.loadGeneSearchKeyword()
		}

		
		// populate SEARCH_KEYWORD_TERM
		if(props.get("skip_search_keyword_term").toString().toLowerCase().equals("yes")){
			log.info "Skip loading new records into SEARCH_KEYWORD_TERM  ..."
		}else{
			SearchKeywordTerm skt = new SearchKeywordTerm()
			skt.setSearchapp(searchapp)
			skt.loadSearchKeywordTerm()
		}
	}


	void readIngenuity(File input, File ipaDef, File ipaData, File ipaDataReject, Map entrez){

		println "Entrez: " + entrez.size() + "\t" + entrez["207"]

		if(input.size() >0){
			log.info ("Start processing " + input.toString())
		}else{
			throw new RuntimeException(input.toString() + " is empty")
		}

		StringBuffer sbDef = new StringBuffer()
		StringBuffer sbData = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()

		int lineNo = 0
		Map line = [:]
		input.eachLine {
			if(it.indexOf("ING:") == 0){
				lineNo++
				line[lineNo] = it
			}else{
				line[lineNo] += it
			}
		}


		line.each{ k, v ->

			String [] str =  v.split(/\t/)

			// extract Ingenuity definition
			sbDef.append(str[0] + "\t" + str[1].replace(",", "").trim() + "\n")

			String [] geneList = str[4].replace("\t", " ").replace('"', '').split(/ +/)
			for(int i in 0..geneList.size()-1) {
				String  geneId =  geneList[i].replace(",", "").trim()
				if(entrez[geneId].equals(null)){
					// if no gene symbol for this gene_id from Entrez, then reject it
					sbReject.append(str[0] + "\t" + geneId + "\n")
				}else{
					sbData.append(str[0] + "\t" + geneId + "\t" + entrez[geneId] + "\n")
				}
			}
		}


		if(ipaDef.size() >0){
			ipaDef.delete()
			ipaDef.createNewFile()
		}
		ipaDef.append(sbDef.toString())


		if(ipaData.size() >0){
			ipaData.delete()
			ipaData.createNewFile()
		}
		ipaData.append(sbData.toString())


		if(ipaDataReject.size() >0){
			ipaDataReject.delete()
			ipaDataReject.createNewFile()
		}
		ipaDataReject.append(sbReject.toString())
	}


	void loadIngenuity(Sql biomart, File ipaData, String pathwayDataTable, String organism){
		
		createPathwayDataTable(biomart, pathwayDataTable)

		if(ipaData.size() > 0){
			log.info ("Start loading " + ipaData.toString())
		}else{
			throw new RuntimeException(ipaData.toString() + " is empty")
		}

		String qry = " insert into ${pathwayDataTable}(pathway, gene_id, gene_symbol, organism) values(?, ?, ?, ?)"

		biomart.withTransaction {
			biomart.withBatch(qry, {stmt ->
				ipaData.eachLine {
					String [] str = it.split("\t")
					stmt.addBatch([str[0], str[1], str[2], organism])
				}
			}
			) }
	}


	void createPathwayDataTable(Sql biomart, String pathwayDataTable){

		String qry = """ create table ${pathwayDataTable} (
							pathway  		varchar2(100),
							gene_id			varchar2(20),
							gene_symbol		varchar2(200),
							organism		varchar2(100)
						 )
					"""

		String qry1 = "select count(*)  from user_tables where table_name=?"
		if(biomart.firstRow(qry1, [pathwayDataTable.toUpperCase()])[0] > 0){
			qry1 = "drop table ${pathwayDataTable} purge"
			biomart.execute(qry1)
		}

		biomart.execute(qry)
	}


	Map getGeneId(Sql biomart, String taxId){

		Map geneList = [:]
		String qry = "select gene_symbol, gene_id from gene_info where tax_id=?"
		biomart.eachRow(qry, [taxId]) {
			String geneId = it.gene_id
			String geneSymbol = it.gene_symbol
			geneList[geneId] = geneSymbol
		}
		return geneList
	}


	Map getPathwayId(Sql deapp, String source){

		Map pathwayId = [:]
		String qry = "select externalid, id from de_pathway where source=?"
		deapp.eachRow(qry, [source]) {
			pathwayId[it.externalid] = it.id
		}
		return pathwayId
	}
}
