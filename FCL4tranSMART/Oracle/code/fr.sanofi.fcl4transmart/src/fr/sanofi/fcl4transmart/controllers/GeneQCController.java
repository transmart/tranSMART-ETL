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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Vector;

import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;

public class GeneQCController {
	private DataTypeItf dataType;
	public GeneQCController(DataTypeItf dataType){
		this.dataType=dataType;
	}
	public HashMap<String, Double> getFileValues(String probeId){
		HashMap<String, Double> filesValues=new HashMap<String, Double>();
		File rawFile=((GeneExpressionData)this.dataType).getRawFile();
		for(String sample: FileHandler.getSamplesId(rawFile)){
			filesValues.put(sample, FileHandler.getIntensity(rawFile, sample, probeId));
		}
		return filesValues;
	}
	public HashMap<String, Double> getDbValues(String probeId){
		HashMap<String, Double> dbValues=new HashMap<String, Double>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("SELECT sample_cd FROM de_subject_sample_mapping WHERE trial_name='"+this.dataType.getStudy().toString().toUpperCase()+"'");
			Vector<String> samples=new Vector<String>();
			while(rs.next()){
				samples.add(rs.getString("sample_cd"));
			}
			for(String sample: samples){
				rs=stmt.executeQuery("select raw_intensity from de_subject_microarray_data where probeset_id in (select probeset_id from de_mrna_annotation where probe_id='"+probeId+"') and patient_id in(select patient_id from de_subject_sample_mapping where trial_name='"+this.dataType.getStudy().toString().toUpperCase()+"' and sample_cd='"+sample+"')");
				if(rs.next()){
					dbValues.put(sample, rs.getDouble("raw_intensity"));
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
