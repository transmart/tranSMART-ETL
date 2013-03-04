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


package com.recomdata.pipeline.annotation

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

import com.recomdata.pipeline.util.Util
import groovy.sql.Sql


class GexGPL {

    private static final Logger log = Logger.getLogger(GexGPL)

    Sql biomart
    String annotationTable, gplFilePattern

    static main(args) {

	PropertyConfigurator.configure("conf/log4j.properties")

	Util util = new Util()

	GexGPL gpl = new GexGPL()

	Properties props = Util.loadConfiguration("conf/GexGPL.properties")

	Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

	gpl.loadGexGPL(props, biomart)
    }


    void loadGexGPL(Properties props, Sql biomart){
	if(props.get("skip_annotation_loader").toString().toLowerCase().equals("yes")){
	    log.info "Skip loading GPL GeX annotation file(s) ..."
	}else{
	    setSql(biomart)
	    setAnnotationTable(props.get("annotation_table"))
	    setGPLFilePattern(props.get("file_pattern"))
	    if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
		log.info "Start recreating annotation table ${props.get("annotation_table")} for GPL GeX annotation file(s) ..."
		createAnnotationTable()
	    }
	    String annotationSourceDirectory = props.get("source_directory")
	    String [] gplList = props.get("gpl_list").split(/\,/)
	    gplList.each {
		File annotationFile = new File(annotationSourceDirectory + File.separator + it)
		loadGexGPLs(props, annotationFile)
	    }
	}
    }



    /**
     * Process GPL Gene Expression annotation files
     *
     * @param input		a GPL annotation file or a directory with GPL annotation files
     */
    void loadGexGPLs(Properties props, File input){

	String columnMap = props.get("column_map")

	if(input.isDirectory()){
	    input.eachFile {
		if((it.toString().indexOf(gplFilePattern) != -1)){
		    log.info "Start loading GPL GeX annotation file ${it.toString()} ..."
		    if(columnMap.equals(null)){
			loadGPL(it)
		    } else {
			loadGPL(props, it)
		    }
		}
	    }
	} else {
	    if(input.isFile()) {
		log.info "Start loading GPL GeX annotation file: ${input.toString()} ..."
		if(columnMap.equals(null)) {
		    loadGPL(input)
		} else {
		    loadGPL(props, input)
		}
	    } else {
		log.error "Neither a directory nor a file: " + input.toString()
	    }
	}
    }


    void loadGPL(Properties props, File input){

	Map columnMap = Util.convertStringToMap(props.get("column_map"))

	Util.printMap(columnMap)
	int idxProbeId = Integer.parseInt(columnMap["probe_id"]) - 1
	int idxSpecies = Integer.parseInt(columnMap["species"]) - 1
	int idxGeneSymbol = Integer.parseInt(columnMap["gene_symbol"]) - 1
	int idxGeneId = Integer.parseInt(columnMap["entrez_gene_id"]) - 1
	int idxGeneTitle = Integer.parseInt(columnMap["gene_title"]) - 1
	println "$idxProbeId  $idxSpecies  $idxGeneSymbol  $idxGeneId  $idxGeneTitle"

	StringBuffer sb = new StringBuffer()
	input.eachLine {
	    if((it.indexOf("#")!=0) && (it.indexOf("ID")!=0)){
		String [] str = it.split(props.get("field_separator"))
		
		if(!str[idxProbeId].equals(null)) sb.append(str[idxProbeId] + "|")
		else sb.append("|")
		
		if(!str[idxSpecies].equals(null)) sb.append(str[idxSpecies] + "|")
		else sb.append("|")
		
		if(str.size() > idxGeneId && !str[idxGeneId].equals(null)) sb.append(str[idxGeneId] + "|")
		else sb.append("|")
		
		if(str.size() > idxGeneSymbol && !str[idxGeneSymbol].equals(null)) sb.append(str[idxGeneSymbol] + "|")
		else sb.append("|")
		
		if(str.size() > idxGeneTitle && !str[idxGeneTitle].equals(null)) sb.append(str[idxGeneTitle] + "\n")
		else sb.append("\n")
		
		//println str[idxProbeId] + "\t" + str[idxSpecies] + "\t" + str[idxGeneId] + "\t" + str[idxGeneSymbol] + "\t" + str[idxGeneTitle]
	    }
	}
	println sb.toString()
    }


    /**
     * Process GPL annotation file
     *
     */
    void loadGPL(File input){
	Map columnMap = [:]
	String platform = input.toString().split(/\./)[1]
	log.info "Input File: " + input.toString() + "\t Platform: " + platform
	if(isGexGPLAnnotationExist(platform)) {
	    log.warn "Annotaion data for ${input.toString()} already exist."
	    return
	}
	// store records need to be loaded
	File output = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".tsv")
	log.info "Output file: " + output.toString()
	if(output.size() > 0){
	    output.delete()
	}
	// store records need to be manually checked
	File reject = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".reject")
	log.info "File for rejected records: " + reject.toString()
	if(reject.size() > 0){
	    reject.delete()
	}
	// store discarded records, either control probes or w/o gene association
	File discard = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".discard")
	log.info "File for rejected records: " + discard.toString()
	if(discard.size() > 0){
	    discard.delete()
	}
	// pick up needed columns from annotation file
	StringBuffer sb = new StringBuffer()
	StringBuffer sbReject = new StringBuffer()
	StringBuffer sbDiscard = new StringBuffer()
	if(input.size() > 0){
	    int line = 0
	    input.eachLine {
		//println it
		line++
		String [] str = it.split("\t")
		int numColumns = str.size()
		if(line==1){
		    for(int i in 0..str.size()-1) {
			println i + "\t" + str[i]
			if(str[i].toUpperCase().equals("ID")) columnMap["ID"] = i
			if(str[i].toUpperCase().equals("GENE_SYMBOL") ||
			str[i].toUpperCase().equals("SYMBOL")) columnMap["GENE_SYMBOL"] = i
			if(str[i].toUpperCase().equals("GENE_NAME") ||
			str[i].toUpperCase().equals("")) columnMap["GENE_NAME"] = i
			if(str[i].toUpperCase().equals("GENE ID") ||
			str[i].toUpperCase().equals("GENE")) columnMap["GENE_ID"] = i
		    }
		} else{
		    String id = "", geneName = "", geneSymbol = "", geneId = ""
		    //if(!columnMap["ID"].equals(null) && numColumns > Integer.parseInt(columnMap["ID"]))
		    if(!columnMap["ID"].equals(null) && numColumns > columnMap["ID"])
			id = str[columnMap["ID"]].trim()
		    //if(!columnMap["GENE_SYMBOL"].equals(null) && numColumns > Integer.parseInt(columnMap["GENE_SYMBOL"]))
		    if(!columnMap["GENE_SYMBOL"].equals(null) && numColumns > columnMap["GENE_SYMBOL"])
			geneName = str[columnMap["GENE_SYMBOL"]].trim()
		    //if(!columnMap["GENE_NAME"].equals(null) && numColumns > Integer.parseInt(columnMap["GENE_NAME"]))
		    if(!columnMap["GENE_NAME"].equals(null) && numColumns > columnMap["GENE_NAME"])
			geneSymbol = str[columnMap["GENE_NAME"]].trim()
		    //if(!columnMap["GENE_ID"].equals(null) && numColumns > Integer.parseInt(columnMap["GENE_ID"]))
		    if(!columnMap["GENE_ID"].equals(null) && numColumns > columnMap["GENE_ID"])
			geneId = str[columnMap["GENE_ID"]].trim()
		    if((geneName.size() >0) || (geneSymbol.size() > 0)) {
			if(geneId.indexOf("|") != -1) {
			    String [] names = geneName.split(/\|/)
			    String [] symbols = geneSymbol.split(/\|/)
			    String [] geneIds = geneId.split(/\|/)
			    if((names.size() == symbols.size()) && (names.size() == geneIds.size())){
				//println "$id \t $geneName \t $geneSymbol \t $geneId"
				for(int i in 0..names.size()-1){
				    //println "$platform\t$id\t${names[i]}\t${symbols[i]}\t${geneIds[i]}\t$platform"
				    sb.append("$platform\t$id\t${names[i]}\t${symbols[i]}\t${geneIds[i]}\t$platform\n")
				}
			    }else{
				//println "$id \t $geneName \t $geneSymbol \t $geneId"
				sbReject.append(it)
			    }
			} else{
			    //println "$platform\t$id\t$geneName\t$geneSymbol\t$geneId\t$platform"
			    sb.append("$platform\t$id\t$geneName\t$geneSymbol\t$geneId\t$platform\n")
			}
		    }else{
			sbDiscard.append(it)
		    }
		}
	    }
	}else{
	    log.error("Empty file: " + input.toString())
	}
	// write data from StringBuffer to files
	if(sb.size() > 0) output.append(sb.toString())
	if(sbReject.size() > 0) reject.append(sbDiscard.toString())
	if(sbDiscard.size() > 0) discard.append(sbDiscard.toString())
	// load probe records with gene symbol and gene id into database
	insertGexGPL(output)
    }


    /**
     *  Load processed Affymetrix annotation data into database
     *
     * @param input
     */
     void insertGexGPL(File input){
         if(input.size() >0){
             log.info "Start loading GPL annotation data ..." 
             biomart.withTransaction {
                 biomart.withBatch("insert into $annotationTable (platform,probe_id,gene_symbol,gene_descr,gene_id) values (?,?,?,?,?)",
                     { ps -> input.eachLine{
			 String [] str = it.split(/\t/)
			 ps.addBatch([str[0], str[1], str[2], str[3], str[4]])
                     	}
                     }) 
             }
         }else{
             log.error("Empty file: " + input.toString())
         }
     }
     
    
     boolean isGexGPLAnnotationExist(String platform){
	 String qry = "select count(*) from $annotationTable where upper(platform)=?"
	 if(biomart.firstRow(qry, [platform.toUpperCase()])[0] > 0){
	     return true
	 } else {
	     return false
	 }
     }
     
     
    /**
     *  The format for Affymetrix GeX Annotation file should be as the following:
     *
     0	Probe Set ID
     1	Gene Symbol
     2	Gene Title
     3	Species Scientific Name
     4	Sequence Type
     5	Sequence Source
     6	Transcript ID[Array Design]
     7	Target Description
     8	Representative Public ID
     9	Archival UniGene Cluster
     10	UniGene ID
     11	Alignments
     12	Chromosomal Location
     13	Unigene Cluster Type
     14	Ensembl
     15	Entrez Gene
     16	SwissProt
     17	EC
     18	OMIM
     19	RefSeq Protein ID
     20	RefSeq Transcript ID
     21	FlyBase
     22	AGI
     23	WormBase
     24	MGI Name
     25	RGD Name
     26	SGD accession number
     27	Gene Ontology Biological Process
     28	Gene Ontology Cellular Component
     29	Gene Ontology Molecular Function
     30	Pathway
     31	InterPro
     32	Trans Membrane
     33	QTL
     34	Annotation Transcript Cluster
     35	Transcript Assignments
     * @param header
     * @return
     */
    boolean isCorrectFormat(String header){
	String [] str = header.split("\t")
	if(!str[0].toString().toUpperCase().equals("Probe Set ID".toUpperCase())) {
	    log.error("Actual header didn't match the expected one: ${str[0]} vs 'Probe Set ID'.")
	    return false
	} else if(!str[1].toString().toUpperCase().equals("Gene Symbol".toUpperCase())) {
	    log.error("Actual header didn't match the expected one: ${str[1]} vs 'Gene Symbol'.")
	    return  false
	} else if(!str[2].toString().toUpperCase().equals("Gene Title".toUpperCase())) {
	    log.error("Actual header didn't match the expected one: ${str[2]} vs 'Gene Title'.")
	    return false
	} else if(!str[3].toString().toUpperCase().equals("Species Scientific Name".toUpperCase())) {
	    log.error("Actual header didn't match the expected one: ${str[3]} vs 'Species Scientific Name'.")
	    return false
	} else if(!str[15].toString().toUpperCase().equals("Entrez Gene".toUpperCase())) {
	    log.error("Actual header didn't match the expected one: ${str[15]} vs 'Entrez Gene'.")
	    return false
	} else {
	    return true
	}
    }


     void createAnnotationTable(){
         String qry = "select count(*) from user_tables where table_name=?"
         if(biomart.firstRow(qry, [annotationTable.toUpperCase()])[0] > 0){
             log.info "The existing table $annotationTable will be dropped ... "
             qry = "drop table $annotationTable purge"
             biomart.execute(qry)
         }
         log.info "Start creating table $annotationTable ..."
         qry = """ create table $annotationTable(
                     platform 	varchar2(100),
                     species  	varchar2(100),
                     probe_id 	varchar2(100),
                     gene_symbol varchar2(100),
                     gene_descr  varchar2(1000),
                     gene_id    	varchar2(20)
                     )"""
         biomart.execute(qry)
     }


    void setAnnotationTable(String annotationTable){
	this.annotationTable = annotationTable
    }


    void setGPLFilePattern(String gplFilePattern){
	this.gplFilePattern = gplFilePattern
    }


    void setSql(Sql biomart){
	this.biomart = biomart
    }
}