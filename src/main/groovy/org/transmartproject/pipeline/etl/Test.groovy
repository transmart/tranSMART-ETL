package org.transmartproject.pipeline.etl

class Test {

	static main(args) {
		
		CommandLineTool d = new CommandLineTool();
		d.main(args)
		print "test"
		print System.getProperty('line.separator')
		
	  
		def m = "34A" =~ /^(\d+)(A|B){0,1}$/
		println m 
		println "size " +m.size()
		println m[0][1].toInteger()
		def dataLabelSourceType = (m[0][2] in ['A', 'B'])?m[0][2]:'A'
		println dataLabelSourceType
		def cat_cd="Sample_Factors+Demographics+Country_Name"
		 cat_cd = (cat_cd =~ /^(.+)\+([^\+]+?)$/).replaceFirst('$1+DATALABEL+$2')
		 
		 println cat_cd
	}

}
