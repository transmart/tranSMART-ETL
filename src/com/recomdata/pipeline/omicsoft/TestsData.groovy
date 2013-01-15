package com.recomdata.pipeline.omicsoft

import groovy.sql.Sql;

import org.apache.log4j.Logger;
import org.apache.log4j.Level

class TestsData {
 
	private static final Logger log = Logger.getLogger(TestsData)

	Sql sql
	String tableName, platform, GSEName, suffix

	TestsData (Level logLevel){
		log.setLevel(logLevel)
	}


	void loadTestsDataDirectory(File sourceDirectory){
		if(sourceDirectory.isDirectory()){
			log.info "Start processing the directory: " + sourceDirectory.toString()
			sourceDirectory.eachFile {
				if(it.toString().indexOf(suffix) != -1){
					File output = loadTestsDataFile(it)
					loadTestsData(output)
				}
			}
		} else{
			File output = loadTestsDataFile(sourceDirectory)
			loadTestsData(output)
		}
	}


	/**
	 *  Reformat *.TESTS.DATA.TXT file and add (GSE# platform) to each line, and reformmatted file
	 *  will be called (GSE#, platform).tsv and stored in the same directory
	 *   
	 * @param input		point to .TESTS.DATA.TXT File object
	 * @return			formatted file object named as "(GSE# platform).TXT" and used for loading
	 */
	File loadTestsDataFile(File input){

		String [] str = []
		boolean isContinue = false

		File output = new File(input.toString().split(/\./)[0].replace(" ", "_") + ".tsv")
		if(output.size() >0){
			output.delete()
			output.createNewFile()
		}

		//String platform = input.getName().split(/\./)[0].replace(" ", "\t")
		platform = input.getName().split(/\./)[0].split(" ")[1].trim()
		GSEName = input.getName().split(/\./)[0].split(" ")[0].trim()

		if(platform.size() > 0){
			setPlatform(platform)
		} else {
			log.error("Platform name is empty")
		}


		if(GSEName.size() > 0){
			setGSEName(GSEName)
		} else {
			log.error("GSE name is empty")
		}

		StringBuffer sb = new StringBuffer()
		if(input.size() > 0){
			log.info "Start transforming file: " + input.toString()
			int index = 0
			input.eachLine{
				index++
				if(it.indexOf("ID\t") == 0) {
					if(isCorrectColumn(it)){
						isContinue = true
					} else{
						log.error("Columns didn't match the expected, please double check the file:" + input.toString())
					}
				}
				if(isContinue && (it.indexOf("ID\t") != 0)){
					//if(index<5)
					sb.append(GSEName + "\t" + platform + "\t" + it + "\n")
				}
			}
		} else {
			log.error("Empty file: " + input.toString())
		}
		output.append(sb.toString())

		return output
	}


	/**
	 *   Load formatted *.TESTS.DATA.TXT into the temporary table TESTS.
	 *   If there is a records for (GSE#, platform) in this table, this
	 *   file will be skipped. 
	 *   
	 * @param input		reformatted *.TESTS.DATA.TXT file 
	 */
	void loadTestsData(File input){

		if(input.size() > 0){
			if(isTestsDataExist(GSEName, platform)){
				log.warn("Exist TESTS data for:  " + GSEName + ":" + platform)
			} else{
				log.info "Start loading: " + input.toString()
				sql.withTransaction {
					sql.withBatch(100, "insert into $tableName(name,platform,id,test,probeset,RawPValue,AdjustedPValue,Estimate,FoldChange,MaxLSMean) values (?,?,?,?,?,?,?,?,?,?)", { ps ->
						input.eachLine{
							String [] str = it.split(/\t/)
							if(str[5].equals(".") || str[5].equals("NaN")) str[5] = ""
							if(str[6].equals(".") || str[6].equals("NaN")) str[6] = ""
							if(str[7].equals(".") || str[7].equals("NaN")) str[7] = ""
							if(str[8].equals(".") || str[8].equals("NaN")) str[8] = ""
							if(str[9].equals(".") || str[9].equals("NaN")) str[9] = ""
							ps.addBatch([
								str[0],
								str[1],
								str[2],
								str[3],
								str[4],
								str[5],
								str[6],
								str[7],
								str[8],
								str[9]
							])
						}
					})
				}
			}
		} else {
			log.error("Empty file: " + input.toString())
		}
	}


	boolean isTestsDataExist(String GSEName, String platform){
		String qry = "select count(1) from $tableName where name=? and platform=?"
		if(sql.firstRow(qry, [GSEName, platform])[0] > 0) return true
		else return false
	}


	/**
	 *  Check if columns are as the expected:
	 *  
	 0	ID
	 1	ContrastName
	 2	VariableName
	 3	RawPValue
	 4	AdjustedPValue
	 5	Estimate
	 6	FoldChange
	 7	MaxLSMean
	 * @param firstLine
	 * @return
	 */
	boolean isCorrectColumn(String header){

		boolean flag = true

		String expectedheader = "ID,ContrastName,VariableName,RawPValue,AdjustedPValue,Estimate,FoldChange,MaxLSMean"
		String [] expected = [
			"ID",
			"ContrastName",
			"VariableName",
			"RawPValue",
			"AdjustedPValue",
			"Estimate",
			"FoldChange",
			"MaxLSMean"
		]

		String [] str = header.split("\t")
		for(int i in 0..str.size()-1){
			if(!str[i].toString().toUpperCase().equals(expected[i].toString().toUpperCase())) {
				println str[i].toString().toUpperCase()
				println expected[i].toString().toUpperCase()
				log.info "Actual Header: ${header.replace('\t', ',')}"
				log.info ("Expected header: $expectedheader")
				log.error("The file's actual header didn't match the expected one.")
				flag = false
			}
		}
		return flag
	}


	Sql setSql(Sql sql){
		this.sql = sql
	}


	/**
	 *   Table is used to load *.TESTS.DATA.TXT and the default table name 
	 *   is "TESTS".
	 *    
	 * @param tableName
	 */
	void setTableName(String tableName){
		this.tableName = tableName
	}


	void setPlatform(String platform){
		this.platform = platform
	}


	void setGSEName(String GSEName){
		this.GSEName = GSEName
	}


	void setSuffix(String suffix){
		this.suffix = suffix
	}


}
