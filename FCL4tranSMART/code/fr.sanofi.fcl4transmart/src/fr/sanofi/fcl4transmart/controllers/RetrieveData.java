/*******************************************************************************
 * FC&L4tranSMART - Framework Curation And Loading For tranSMART
 * 
 * Copyright (c) 2012 Sanofi.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Sanofi - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

public class RetrieveData {
	public static String getConnectionString(){
		return "jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
	}
	public static Vector<String> getTaxononomy(){
	    Vector<String> taxons=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT distinct taxon_name from bio_taxonomy");

		    while(rs.next()){
		    	taxons.add(rs.getString("taxon_name"));
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return taxons;
		}
		catch(ClassNotFoundException cnfe){
			return taxons;
		}
		return taxons;
	}
	public static String retrieveTitle(String study){
		String title="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT title from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	title=rs.getString("title");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(title!=null){
				return title;
		}
		else{
			return "";
		}
	}
	public static String retrieveDescription(String study){
		String description="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT description from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	description=rs.getString("description");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(description!=null){
			return description;
		}
		else{
			return "";
		}
	}
	public static String retrieveDesign(String study){
		String design="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT design from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	design=rs.getString("design");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(design!=null){
			return design;
		}
		else{
			return "";
		}
	}
	public static String retrieveOwner(String study){
		String owner="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT study_owner from bio_clinical_trial where trial_number='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	owner=rs.getString("study_owner");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(owner!=null){
			return owner;
		}
		else{
			return "";
		}
	}
	public static String retrieveInstitution(String study){
		String institution="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT institution from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	institution=rs.getString("institution");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(institution!=null){
			return institution;
		}
		else{
			return "";
		}
	}	
	public static String retrieveCountry(String study){
		String country="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT country from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	country=rs.getString("country");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(country!=null){
			return country;
		}
		else{
			return "";
		}
	}
	public static String retrieveAccessType(String study){
		String access_type="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT access_type from bio_experiment where accession='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	access_type=rs.getString("access_type");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(access_type!=null){
			return access_type;
		}
		else{
			return "";
		}
	}
	public static String retrievePhase(String study){
		String phase="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT study_phase from bio_clinical_trial where trial_number='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	phase=rs.getString("study_phase");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(phase!=null){
			return phase;
		}
		else{
			return "";
		}
	}
	public static String retrieveNumber(String study){
		String number="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT number_of_patients from bio_clinical_trial where trial_number='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	number=rs.getString("number_of_patients");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(number!=null){
			return number;
		}
		else{
			return "";
		}
	}
	public static String retrieveOrganism(String study){
		String organism="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT taxon_name from bio_taxonomy where bio_taxonomy_id in(select bio_taxonomy_id from bio_data_taxonomy where etl_source='"+study.toUpperCase()+"')");

		    if(rs.next()){
		    	organism=rs.getString("taxon_name");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(organism!=null){
			return organism;
		}
		else{
			return "";
		}
	}
	public static String retrievePubmed(String study){
		String pubmed="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT location from biomart.bio_content where study_name='"+study.toUpperCase()+"' and repository_id in (select bio_content_repo_id from biomart.bio_content_repository where repository_type='PubMed')");

		    if(rs.next()){
		    	pubmed=rs.getString("location");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(pubmed!=null){
			return pubmed;
		}
		else{
			return "";
		}
	}
	public static String retrieveTopNode(String study){
		String topNode="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT path from i2b2_tags where tag='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	topNode=rs.getString("path");
		    }
		    con.close();
				
		}catch(SQLException sqle){
			return "";
		}
		catch(ClassNotFoundException cnfe){
			return "";
		}
		if(topNode!=null){
			return topNode;
		}
		else{
			return "";
		}
	}
	public static boolean isLoaded(String study){
		boolean isLoaded=false;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT * from i2b2_tags where tag='"+study.toUpperCase()+"'");

		    if(rs.next()){
		    	isLoaded=true;
		    }
		    else{
		    	rs = stmt.executeQuery("SELECT * from i2b2 where sourcesystem_cd='"+study.toUpperCase()+"'");
		    	if(rs.next()){
		    		isLoaded=true;
		    	}
		    }
		    
		    con.close();
				
		}catch(SQLException sqle){
			return false;
		}
		catch(ClassNotFoundException cnfe){
			return false;
		}
		return isLoaded;
	}
	public static boolean testBiomartConnection(String dbServer, String dbName, String dbPort, String biomartUser, String biomartPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(connection, biomartUser, biomartPwd);
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testMetadataConnection(String dbServer, String dbName, String dbPort, String metadataUser, String metadataPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(connection, metadataUser, metadataPwd);
			con.close();
		}
		catch(SQLException e){
			
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testDemodataConnection(String dbServer, String dbName, String dbPort, String demodataUser, String demodataPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(connection, demodataUser, demodataPwd);
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testDeappConnection(String dbServer, String dbName, String dbPort, String deappUser, String deappPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
		
			Connection con = DriverManager.getConnection(connection, deappUser, deappPwd);
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
			return false;
		}
		return true;
	}
	public static boolean testTm_lzConnection(String dbServer, String dbName, String dbPort, String tm_lzUser, String tm_lzPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(connection, tm_lzUser, tm_lzPwd);
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testTm_czConnection(String dbServer, String dbName, String dbPort, String tm_czUser, String tm_czPwd){
		String connection="jdbc:oracle:thin:@"+dbServer+":"+dbPort+":"+dbName;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(connection, tm_czUser, tm_czPwd);
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testBiomartConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testMetadataConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			con.close();
		}
		catch(SQLException e){
			
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testDemodataConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testDeappConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
		
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
			return false;
		}
		return true;
	}
	public static boolean testTm_lzConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getTm_lzUser(), PreferencesHandler.getTm_lzPwd());
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static boolean testTm_czConnection(){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getTm_czUser(), PreferencesHandler.getTm_czPwd());
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	public static Vector<String> getStudiesIds(){
		Vector<String> ids=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select distinct sourcesystem_cd from i2b2");
			while(rs.next()){
				String id=rs.getString("sourcesystem_cd");
				if(id!=null) ids.add(id);
			}
			con.close();
		}catch(SQLException e){
			e.printStackTrace();
			return  null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return ids;
	}
	/*public static Vector<String> getTopFolders(){
		Vector<String> topFolders=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select distinct c_name from table_access");
			while(rs.next()){
				String topFolder=rs.getString("c_name");
				if(topFolder!=null) topFolders.add(topFolder);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
			return  null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return topFolders;
	}*/
	public static Vector<String> getStudies(){
		Vector<String> studies=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select concept_path  from concept_counts where parent_concept_path not in(select concept_path from concept_counts)");
			while(rs.next()){
				String study=rs.getString("concept_path");
				studies.add(study);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return studies;
	}
	/*public static Vector<String> getSubFolders(){
		Vector<String> subFolders=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("(select concept_path from concept_dimension) MINUS (select concept_path from concept_counts)");
			while(rs.next()){
				String subFolder=rs.getString("concept_path");
				if(subFolder!=null) subFolders.add(subFolder);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return subFolders;
	}
	public static Vector<String> getStudies(String parentPath){
		Vector<String> studies=new Vector<String>();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select distinct concept_path from concept_counts where parent_concept_path='"+parentPath+"'");
			while(rs.next()){
				String study=rs.getString("concept_path");
				if(study!=null) studies.add(study);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return studies;
	}*/
	public static String getIdFromPath(String path){
		String id="";
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select distinct sourcesystem_cd from concept_dimension where concept_path='"+path+"'");
			if(rs.next()){
				id=rs.getString("sourcesystem_cd");
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return id;
	}
	public static int getClinicalPatientNumber(String study){
		int n=0;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select count(distinct patient_num) from patient_trial where trial='"+study.toUpperCase()+"'");
			if(rs.next()){
				n=rs.getInt(1);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return n;
	}
	public static int getGenePatientNumber(String study){
		int n=0;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select count(distinct subject_id) from de_subject_sample_mapping where trial_name='"+study.toUpperCase()+"'");
			if(rs.next()){
				n=rs.getInt(1);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return n;
	}
	public static int getGeneProbeNumber(String study){
		int n=0;
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select count(distinct probeset_id) from de_subject_microarray_data where trial_name='"+study.toUpperCase()+"'");
			if(rs.next()){
				n=rs.getInt(1);
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return n;
	}
}