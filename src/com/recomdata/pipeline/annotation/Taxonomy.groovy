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

import java.util.Properties;

import com.recomdata.pipeline.util.Util
import groovy.sql.Sql;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator


class Taxonomy {

	private static final Logger log = Logger.getLogger(Taxonomy)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()

		Properties props = Util.loadConfiguration("conf/loader.properties")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

		Taxonomy tax = new Taxonomy()
		tax.loadTaxonomyData(props, biomart)
	}


	void loadTaxonomyData(Properties props, Sql biomart){

		if(props.get("skip_taxonomy_name").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Taxonomy name ..."
		}else{
			File taxonomy = new File(props.get("taxonomy_source"))
			String taxonomyTable = props.get("taxonomy_name_table")
			createTaxonomyTable(taxonomyTable, biomart)
			loadTaxonomy(taxonomy, taxonomyTable, biomart)
		}
	}


	/**
	 *  9606 (Homo sapiens); 10116 (Rattus norvegicus); 10090 (Mus musculus)
	 *  
	 * Taxonomy names file has these fields:
	 * 
	 * 	tax_id					-- the id of node associated with this name
	 * 	name_txt				-- name itself
	 * 	unique name				-- the unique variant of this name if name not unique
	 * 	name class				-- (synonym, common name, ...)
	 * 
	 * @param taxonomy
	 */
	void loadTaxonomy(File taxonomy, String taxonomyTable,  Sql biomart){

		String qry = "insert into $taxonomyTable (tax_id, name_txt, unique_name, name_class) values (?, ?, ?, ?)"

		if(taxonomy.size() > 0){
			log.info "Start loading Taxnomy file: " + taxonomy.toString()
			biomart.withTransaction {
				biomart.withBatch(20, qry,  { stmt ->
					taxonomy.eachLine {
						String [] str = it.split(/\|/)
						stmt.addBatch([
							str[0].trim(),
							str[1].trim(),
							str[2].trim(),
							str[3].trim()
						])
					}
				})
			}
			log.info "End loading Taxnomy file: " + taxonomy.toString()
		}else{
			log.error("Taxonomy file is empty.")
			return
		}
	}


	void createTaxonomyTable(String taxonomyTable, Sql biomart){

		String qry = "select count(1) from user_tables where table_name=upper(?)"

		if(biomart.firstRow(qry, [taxonomyTable])[0] > 0){
			log.info "Drop table $taxonomyTable ..."
			qry = "drop table $taxonomyTable purge"
			biomart.execute(qry)
		}

		log.info "Create table $taxonomyTable ..."
		qry = """ create table $taxonomyTable (
							tax_id   number(10,0),
							name_txt   varchar2(200),
							unique_name   varchar2(100),
							name_class   varchar2(100)
                        ) """
		biomart.execute(qry)
	}

}
