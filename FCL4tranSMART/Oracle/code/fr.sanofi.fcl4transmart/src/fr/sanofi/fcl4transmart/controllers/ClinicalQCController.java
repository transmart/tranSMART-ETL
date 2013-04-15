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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;

public class ClinicalQCController {
	private DataTypeItf dataType;
	public ClinicalQCController(DataTypeItf dataType){
		this.dataType=dataType;
	}
	public HashMap<String, String> getFileValues(String subject){
		HashMap<String, String> filesValues=new HashMap<String, String>();
		for(String dataLabel: FileHandler.getDataLabelsForQC(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles())){
			String value=FileHandler.getValueForSubjectForQC(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles(), subject, dataLabel, ((ClinicalData)this.dataType).getWMF());
			if(dataLabel.split(":", -1).length==3){
				dataLabel=FileHandler.getValueForSubjectByColumn(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles(), subject, dataLabel.split(":", -1)[2], dataLabel.split(":", -1)[1], ((ClinicalData)this.dataType).getWMF());	
			}
			filesValues.put(dataLabel, value);
		}
		return filesValues;
	}
	public HashMap<String, String> getDbValues(String subject){
		HashMap<String, String> dbValues=new HashMap<String, String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			Statement stmt2= con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT * from OBSERVATION_FACT where SOURCESYSTEM_CD='"+this.dataType.getStudy().toString().toUpperCase()+":"+subject+"'");
		    while(rs.next()){
		    	String nval=rs.getString("VALTYPE_CD");
		    	String concept_cd=rs.getString("CONCEPT_CD");
		    	if(nval.compareTo("N")==0){
		    		String value=String.valueOf(rs.getDouble("NVAL_NUM"));
		    		ResultSet rs2=stmt2.executeQuery("select NAME_CHAR from CONCEPT_DIMENSION where CONCEPT_CD='"+concept_cd+"'");
		    		if(rs2.next()){
		    			dbValues.put(rs2.getString("NAME_CHAR"), value);
		    		}
		    		rs2.close();
		    	}
		    	else if(nval.compareTo("T")==0){
		    		String tval=rs.getString("TVAL_CHAR");
		    		if(tval.compareTo("EXP:PUBLIC")!=0){
		    			ResultSet rs2=stmt2.executeQuery("select CONCEPT_PATH from CONCEPT_DIMENSION where CONCEPT_CD='"+concept_cd+"'");
		    			if(rs2.next()){
		    				String path=rs2.getString("CONCEPT_PATH");
		    				String[] splitedPath=path.split("\\\\", -1);
		    				String key=splitedPath[splitedPath.length-3];
		    				dbValues.put(key, tval);
		    			}
			    		rs2.close();
		    		}
		    	}
		    }
		    
		con.close();
		}catch(SQLException sqle){
			sqle.printStackTrace();
			return null;
		}
		catch(ClassNotFoundException cnfe){
			cnfe.printStackTrace();
			return null;
		}
		return dbValues;
	}
}
