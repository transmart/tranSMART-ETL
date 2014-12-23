/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2012-2014 The TranSMART Foundation
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
