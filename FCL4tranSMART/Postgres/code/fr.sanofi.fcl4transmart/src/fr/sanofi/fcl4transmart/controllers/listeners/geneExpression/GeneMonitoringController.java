/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et D�veloppement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et D�veloppement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers.listeners.geneExpression;

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

import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
/**
 *This class controls the monitoring for gene expression date loading
 */	
public class GeneMonitoringController {
	private DataTypeItf dataType;
	private File logFile;
	public GeneMonitoringController(DataTypeItf dataType){
		this.dataType=dataType;
	}
	/**
	 *Check if a log file exists, which means that a loading have been done
	 */	
	public boolean checkLogFileExists(){
		File logFile=((GeneExpressionData)this.dataType).getLogFile();
		if(logFile.exists()){
			this.logFile=logFile;
			return true;
		}
		return false;
	}
	/**
	 *Checks in the log file if the called KEttle job succeed
	 */	
	public boolean kettleSucceed(){
		if(this.logFile!=null){
			try{
				BufferedReader br = new BufferedReader(new FileReader(this.logFile));
				Pattern pattern=Pattern.compile(".*\\[run i2b2_process_mrna_data\\].*", Pattern.DOTALL);
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
	/**
	 *Check in the database if the called stored procedure succeed, if not returns the oracle error code
	 */	
	public String proceduresError(){
		String procedureErrors="";
		try{
			try {
				Class.forName("org.postgresql.Driver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String connectionString="jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
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
			System.out.println(jobId);
			ResultSet rs=stmt.executeQuery("(select STEP_DESC from CZ_JOB_AUDIT where STEP_STATUS='ERROR' and JOB_ID="+String.valueOf(jobId)+") UNION "+
					"(select STEP_DESC from CZ_JOB_AUDIT where STEP_STATUS='FAIL' and JOB_ID="+String.valueOf(jobId)+")");
			if(rs.next()){
				procedureErrors=rs.getString("STEP_DESC");
				//con.close();
				//return procedureErrors;
			}
	
			rs=stmt.executeQuery("select ERROR_MESSAGE from CZ_JOB_ERROR where JOB_ID="+String.valueOf(jobId));
			if(rs.next()){
				procedureErrors=rs.getString("ERROR_MESSAGE");
			}
			if(procedureErrors.compareTo("User-Defined Exception")==0 || procedureErrors.compareTo("Application raised error")==0){
				rs=stmt.executeQuery("select STEP_DESC from CZ_JOB_AUDIT where JOB_ID="+String.valueOf(jobId)+" and SEQ_ID in(select max(SEQ_ID)-1 from CZ_JOB_AUDIT where JOB_ID="+String.valueOf(jobId)+")");
				if(rs.next()){
					procedureErrors=rs.getString("STEP_DESC");
				}
			}
			con.close();
	    	
			}catch(SQLException sqle){
				sqle.printStackTrace();
			}
		return procedureErrors;
	}
}
