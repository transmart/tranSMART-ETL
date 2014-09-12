/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 * 
 * Copyright 2012-2013 Thomson Reuters
 * 
 * This product includes software developed at Thomson Reuters
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  
 * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/

package org.transmartproject.pipeline.etl


import org.apache.log4j.PropertyConfigurator;

enum LogType { MESSAGE, WARNING, ERROR, DEBUG, PROGRESS }

class Logger {
	
	def config
	File logFile 
	
	Logger(conf) {
		config = conf
		if (! config?.log?.fileName) 
			logFile = new File("tm_clinical_load.log")
		else
		logFile = new File( config?.log?.fileName) 
		
	}
	
	private String timestamp(LogType ltype) {
		def tm = new Date()
		def str = String.format('%tm/%<td %<tT ', tm)
		
		switch (ltype) {
			case LogType.DEBUG:
				str += 'DBG '
				break
			case LogType.WARNING: 
				str += 'WAR '
				break
			case LogType.ERROR: 
				str += 'ERR '
				break
			case LogType.MESSAGE: 
			default:
				str += 'MSG '
		}
		
		return str
	}
	//modified to write to a file instead of console
	void log(LogType ltype, str) {
		if (ltype != LogType.PROGRESS) {
			str = timestamp(ltype) + str
			if (ltype == LogType.ERROR)
				System.err.println(str)
				
			else
				{
				println str 
				logFile<< str+System.getProperty('line.separator')
				}
		}
		else if (config.isInteractiveMode)
			{
				print '\r'+str
			logFile<< str+System.getProperty('line.separator')
			}
	}
	
	void log(str) {
		log(LogType.MESSAGE, str)
	}

}
