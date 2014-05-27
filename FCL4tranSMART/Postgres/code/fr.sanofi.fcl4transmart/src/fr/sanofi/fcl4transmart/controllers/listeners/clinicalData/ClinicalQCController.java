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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
/**
 * This class controls the clinical data quality control step
 */
public class ClinicalQCController {
	private DataTypeItf dataType;
	public ClinicalQCController(DataTypeItf dataType){
		this.dataType=dataType;
	}
	/**
	 * Returns a Hash map containing properties and values for a given subject from data files
	 */
	public HashMap<String, String> getFileValues(String subject){
		HashMap<String, String> filesValues=new HashMap<String, String>();
		for(String dataLabel: FileHandler.getDataLabelsForQC(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles())){
			String value=FileHandler.getValueForSubjectForQC(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles(), subject, dataLabel, ((ClinicalData)this.dataType).getWMF());
			if(dataLabel.split(":", -1).length==3){
				dataLabel=FileHandler.getValueForSubjectByColumn(((ClinicalData)this.dataType).getCMF(), ((ClinicalData)this.dataType).getRawFiles(), subject, dataLabel.split(":", -1)[2], dataLabel.split(":", -1)[1], ((ClinicalData)this.dataType).getWMF());	
			}
			filesValues.put(this.replaceLabel(dataLabel), this.replaceValue(value));
		}
		return filesValues;
	}
	/**
	 * Returns a Hash map containing properties and values for a given subject from database
	 */
	public HashMap<String, String> getDbValues(String subject){
		HashMap<String, String> dbValues=new HashMap<String, String>();
		try{
			Class.forName("org.postgresql.Driver");
			String connectionString="jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			Statement stmt2= con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * from I2B2DEMODATA.OBSERVATION_FACT where patient_num in (select patient_num from I2B2DEMODATA.PATIENT_DIMENSION where sourcesystem_cd ~'"+this.dataType.getStudy().toString().toUpperCase()+":(.*:)*"+subject+"$')");
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
	public String replaceValue(String value){
		Pattern p1=Pattern.compile(".*\\(( )*\\).*");
		Pattern p2=Pattern.compile(".*\\(.*");
		Pattern p3=Pattern.compile(".*\\).*");
		Matcher m1=p1.matcher(value);
		Matcher m2=p2.matcher(value);
		Matcher m3=p3.matcher(value);
		if(m1.matches() || (m2.matches() && !m3.matches())){
			value=value.replaceAll("(", "");
		}
		if(m1.matches() || m3.matches() && ! m2.matches()){
			value=value.replaceAll(")", "");
		}
		value=value.replaceAll("\\|$", "").replaceAll("^\\|", "");
		value=value.replaceAll("\\|", "-");
		value=value.replaceAll("%", "Pct");
		value=value.replaceAll("&", " and ");
		value=value.trim();
		value=value.replaceAll("  ", " ");
		value=value.replaceAll(" ,", ",");
		value=value.replaceAll("\\+", " and");
		value=value.replaceAll("\\\"", "");
		
		return value;
	}
	public String replaceLabel(String label){
		label=label.replaceAll("%", "Pct");
		label=label.replaceAll("&", " and ");
		label=label.replaceAll("\\|", ",");
		label=label.trim();
		label=label.replaceAll("  ", " ");
		label=label.replaceAll(" ,", ",");
		label=label.replaceAll("_", " ");
		
		return label;
		
	}
}
