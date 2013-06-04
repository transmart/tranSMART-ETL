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
package fr.sanofi.fcl4transmart.controllers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
/**
 *This class allows data retrieving from database
 */
public class RetrieveData {
	public static String getConnectionString(){
		return "jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
	}
	/**
	 *Returns a vector with organisms names
	 */
	public static Vector<String> getTaxononomy(){
	    Vector<String> taxons=new Vector<String>();
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study title from its identifier
	 */
	public static String retrieveTitle(String study){
		String title="";
		try{
			Class.forName("org.postgresql.Driver");
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
	
	/**
	 *Retrieves study description from its identifier
	 */
	public static String retrieveDescription(String study){
		String description="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study design from its identifier
	 */
	public static String retrieveDesign(String study){
		String design="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study owner from its identifier
	 */
	public static String retrieveOwner(String study){
		String owner="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study institution from its identifier
	 */
	public static String retrieveInstitution(String study){
		String institution="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study country from its identifier
	 */
	public static String retrieveCountry(String study){
		String country="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study access type from its identifier
	 */
	public static String retrieveAccessType(String study){
		String access_type="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study phase from its identifier
	 */
	public static String retrievePhase(String study){
		String phase="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study number from its identifier
	 */
	public static String retrieveNumber(String study){
		String number="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study organism from its identifier
	 */
	public static String retrieveOrganism(String study){
		String organism="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study pubmed from its identifier
	 */
	public static String retrievePubmed(String study){
		String pubmed="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Retrieves study top node from its identifier
	 */
	public static String retrieveTopNode(String study){
		String topNode="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Checks is a study is loaded
	 */
	public static boolean isLoaded(String study){
		boolean isLoaded=false;
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Checks that the connection to biomart database is available with given parameters
	 */
	public static boolean testBiomartConnection(String dbServer, String dbName, String dbPort, String biomartUser, String biomartPwd){
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), biomartUser, biomartPwd);
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
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), metadataUser, metadataPwd);
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
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), demodataUser, demodataPwd);
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
	public static boolean testDeappConnection(String dbServer, String dbName, String dbPort, String deappUser, String deappPwd){
		try{
			Class.forName("org.postgresql.Driver");
		
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), deappUser, deappPwd);
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
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), tm_lzUser, tm_lzPwd);
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
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), tm_czUser, tm_czPwd);
			con.close();
		}
		catch(SQLException e){
			return false;
		} catch (ClassNotFoundException e2) {
			return false;
		}
		return true;
	}
	/**
	 *Checks that the connection to biomart database is available with parameters from preferences
	 */	
	public static boolean testBiomartConnection(){
		try{
			Class.forName("org.postgresql.Driver");
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
			Class.forName("org.postgresql.Driver");
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
			Class.forName("org.postgresql.Driver");
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
			Class.forName("org.postgresql.Driver");
		
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
			Class.forName("org.postgresql.Driver");
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
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns a vector containing studies identifiers for all loaded studies
	 */	
	public static Vector<String> getStudiesIds(){
		Vector<String> ids=new Vector<String>();
		try{
			Class.forName("org.postgresql.Driver");
			
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
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
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns a vector containing studies paths for all loaded studies
	 */	
	public static Vector<String> getStudies(){
		Vector<String> studies=new Vector<String>();
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
			ResultSet rs=stmt.executeQuery("select c_fullname from i2b2 where c_hlevel=1 and c_comment like 'trial%'");
			while(rs.next()){
				String study=rs.getString("c_fullname");
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
			Class.forName("org.postgresql.Driver");
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
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns the study path for a given identifier
	 */	
	public static String getIdFromPath(String path){
		String id="";
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns the patient count for a clinical study
	 */	
	public static int getClinicalPatientNumber(String study){
		int n=0;
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns the patient count for a gene expression study
	 */	
	public static int getGenePatientNumber(String study){
		int n=0;
		try{
			Class.forName("org.postgresql.Driver");
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
	/**
	 *Returns the probe count for a gene expression study
	 */	
	public static int getGeneProbeNumber(String study){
		int n=0;
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
			Statement stmt = con.createStatement();
			//ResultSet rs=stmt.executeQuery("select count(distinct probeset_id) from de_subject_microarray_data where trial_name='"+study.toUpperCase()+"'");
			
			int partition=-1;
			ResultSet rs=stmt.executeQuery("select distinct partition_id from deapp.de_subject_sample_mapping where trial_name='"+study.toUpperCase()+"'");
			
			if(rs.next()){
				partition=rs.getInt(1);
			}
			if(partition!=-1){
				rs=stmt.executeQuery("select count(distinct probeset_id) from deapp.de_subject_microarray_data_"+partition+" where assay_id in("+
						"select assay_id from deapp.de_subject_sample_mapping where trial_name='"+study.toUpperCase()+"');");
				
				if(rs.next()){
					n=rs.getInt(1);
				}
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
	/**
	 *Returns the tags associated with a given study
	 */	
	public static Vector<Vector<String>> getTags(String topNode){
		Vector<Vector<String>> tags=new Vector<Vector<String>>();
		Vector<String> fields=new Vector<String>();
		Vector<String> values=new Vector<String>();
		tags.add(fields);
		tags.add(values);
		try{
			Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
			
			ResultSet rs=stmt.executeQuery("select tag, tag_type from i2b2_tags where path='"+topNode+"'");
			
			while(rs.next()){
				String field=rs.getString("tag_type");
				String value=rs.getString("tag");
				if(field!=null && value!=null){
					tags.get(0).add(field);
					tags.get(1).add(value);
				}
			}
			con.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e2) {
			e2.printStackTrace();
		}
		return tags;
	}
}
