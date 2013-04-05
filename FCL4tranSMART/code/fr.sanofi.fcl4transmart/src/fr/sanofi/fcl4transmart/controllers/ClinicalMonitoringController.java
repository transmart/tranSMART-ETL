/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et Développement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et Développement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;

public class ClinicalMonitoringController {
	private DataTypeItf dataType;
	private File logFile;
	public ClinicalMonitoringController(DataTypeItf dataType){
		this.dataType=dataType;
	}
	public boolean checkLogFileExists(){
		File logFile=((ClinicalData)this.dataType).getLogFile();
		if(logFile!=null){
			this.logFile=logFile;
			return true;
		}
		return false;
		
	}
	public boolean kettleSucceed(){
		if(this.logFile!=null){
			try{
				BufferedReader br = new BufferedReader(new FileReader(this.logFile));
				Pattern pattern=Pattern.compile(".*Finished job entry \\[run i2b2_load_clinical_data\\] \\(result=\\[true\\]\\)");
				String line="";
				while ((line=br.readLine())!=null){
					Matcher matcher=pattern.matcher(line);
					if(matcher.matches()){
						br.close();
						return true;
					}
				}
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		return false;
	}
	public String proceduresError(){
		String procedureErrors="";
		try{
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getTm_czUser(), PreferencesHandler.getTm_czPwd());
			Statement stmt = con.createStatement();

			int jobId=-1;
			if(this.logFile!=null){
				try{
					BufferedReader br = new BufferedReader(new FileReader(this.logFile));
					String line;
					while ((line=br.readLine())!=null){
						if(line.compareTo("Oracle job id:")==0){
							try{
								jobId=Integer.parseInt(br.readLine());
							}
							catch(Exception e){
								br.close();
								return "";
							}
						}
					}
					br.close();
				}catch (Exception e){
					e.printStackTrace();
				}
			}
			else{
				return "";
			}
			if(jobId==-1){
				return "";
			}
			
			ResultSet rs=stmt.executeQuery("select STEP_DESC from CZ_JOB_AUDIT where STEP_STATUS='ERROR' and JOB_ID="+String.valueOf(jobId));
			if(rs.next()){
				procedureErrors=rs.getString("STEP_DESC");
				con.close();
				return procedureErrors;
			}
	
			rs=stmt.executeQuery("select ERROR_MESSAGE from CZ_JOB_ERROR where JOB_ID="+String.valueOf(jobId));
			if(rs.next()){
				procedureErrors=rs.getString("ERROR_MESSAGE");
			}
			con.close();
	    	
			}catch(SQLException sqle){
				
				sqle.printStackTrace();
			}
		return procedureErrors;
	}
}
