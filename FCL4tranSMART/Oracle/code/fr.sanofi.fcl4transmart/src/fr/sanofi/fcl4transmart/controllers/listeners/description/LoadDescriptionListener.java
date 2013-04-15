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
package fr.sanofi.fcl4transmart.controllers.listeners.description;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.workUI.description.LoadDescriptionUI;

public class LoadDescriptionListener implements Listener{
	private LoadDescriptionUI loadDescriptionUI;
	public LoadDescriptionListener(LoadDescriptionUI loadDescriptionUI){
		this.loadDescriptionUI=loadDescriptionUI;
	}
	@Override
	public void handleEvent(Event event) {
		if(this.loadDescriptionUI.getTopNode().compareTo("")==0){
			this.loadDescriptionUI.displayMessage("Without correct study node, metadata will not appear in dataset explorer");
		}
		if(this.loadDescriptionUI.getTitle().length()>1000){
			this.loadDescriptionUI.displayMessage("Title is limited to 1000 characters");
			return;
		}
		if(this.loadDescriptionUI.getDescription().length()>2000){
			this.loadDescriptionUI.displayMessage("Description is limited to 2000 characters");
			return;
		}
		if(this.loadDescriptionUI.getDesign().length()>2000){
			this.loadDescriptionUI.displayMessage("Design is limited to 2000 characters");
			return;
		}
		if(this.loadDescriptionUI.getOwner().length()>400){
			this.loadDescriptionUI.displayMessage("Owner is limited to 400 characters");
			return;
		}
		if(this.loadDescriptionUI.getInstitution().length()>100){
			this.loadDescriptionUI.displayMessage("Institution is limited to 100 characters");
			return;
		}
		if(this.loadDescriptionUI.getCountry().length()>50){
			this.loadDescriptionUI.displayMessage("Country is limited to 50 characters");
			return;
		}
		if(this.loadDescriptionUI.getAccessType().length()>100){
			this.loadDescriptionUI.displayMessage("Access Type is limited to 100 characters");
			return;
		}
		if(this.loadDescriptionUI.getPhase().length()>100){
			this.loadDescriptionUI.displayMessage("Phase is limited to 100 characters");
			return;
		}
		if(this.loadDescriptionUI.getNumber().compareTo("")!=0){
			try{
				Integer.valueOf(this.loadDescriptionUI.getNumber());
			}
			catch(NumberFormatException e){
				this.loadDescriptionUI.displayMessage("Patient number has to be an integer");
				return;
			}
		}
		if(this.loadDescriptionUI.getPubMedAccession().compareTo("")!=0){
			try{
				Integer.valueOf(this.loadDescriptionUI.getPubMedAccession());
			}
			catch(NumberFormatException e){
				this.loadDescriptionUI.displayMessage("Patient number has to be an integer");
				return;
			}
		}
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
			Statement stmt = con.createStatement();
			
			//remove rows for this study before adding new ones
			ResultSet rs=stmt.executeQuery("delete from bio_clinical_trial where trial_number='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
			rs=stmt.executeQuery("delete from bio_experiment where accession='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
			rs=stmt.executeQuery("delete from bio_content_reference where etl_id_c='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
			rs=stmt.executeQuery("delete from bio_content where file_name='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
			rs=stmt.executeQuery("delete from bio_data_taxonomy where etl_source='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
		    rs = stmt.executeQuery(
		    		"insert into bio_experiment("+
		    		"title,"+ 
					"description,"+ 
					"design,"+
					"primary_investigator,"+ 
					"accession,"+
					"institution,"+
					"country,"+
					"access_type) "+
					"values"+
					"('"+this.loadDescriptionUI.getTitle()+"'"+
					",'"+this.loadDescriptionUI.getDescription()+"'"+
					",'"+this.loadDescriptionUI.getDesign()+"'"+
					",'"+this.loadDescriptionUI.getOwner()+"'"+
					",'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
					",'"+this.loadDescriptionUI.getInstitution()+"'"+
					",'"+this.loadDescriptionUI.getCountry()+"'"+
					",'"+this.loadDescriptionUI.getAccessType()+"'"+
					")"
				);
		
		    	int bio_experiment_id=0;
		    	
		    	rs = stmt.executeQuery("SELECT bio_experiment_id from bio_experiment where accession='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
		    	if(rs.next()){
		    		bio_experiment_id=rs.getInt("bio_experiment_id");
		    	}
		    	else{
		    		return;
		    	}
		    	String number=this.loadDescriptionUI.getNumber();
		    	if(number.compareTo("")==0) number="null";
		    	rs = stmt.executeQuery(
	    			"insert into bio_clinical_trial"+
	    			"(trial_number"+
	    			",study_owner"+
	    			",study_phase"+
	    			",number_of_patients"+
	    			",bio_experiment_id"+
	    			") values("+
	    			"'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
	    			",'"+this.loadDescriptionUI.getOwner()+"'"+
	    			",'"+this.loadDescriptionUI.getPhase()+"'"+
	    			","+number+
	    			","+Integer.toString(bio_experiment_id)+
	    			")"
		    	);
		    	
		    	int pubmedRepository=-1;
		    	try{
		    		rs=stmt.executeQuery("select bio_content_repo_id from bio_content_repository where repository_type='PubMed'");
		    		if(rs.next()){
		    			pubmedRepository=rs.getInt("bio_content_repo_id");
		    		}
		    		else{
			    		this.loadDescriptionUI.displayMessage("Warning: no identifier in database for Pubmed links");
		    		}
		    	}
		    	catch(SQLException noPubmedRep){
		    		//do nothing
		    	}
		    	if(this.loadDescriptionUI.getPubMedAccession()!=null && this.loadDescriptionUI.getPubMedAccession().compareTo("")!=0 && pubmedRepository!=-1){
		    		try{
		    			Integer.valueOf(this.loadDescriptionUI.getPubMedAccession());
		    		}
		    		catch(Exception e){
		    			this.loadDescriptionUI.displayMessage("Warning: Pubmed identifier has to be a number");
		    		}
		    		rs = stmt.executeQuery(
				    	"insert into bio_content"+
				    	"(file_name"+
				    	",repository_id"+
				    	",location"+
				    	",file_type"+
				    	",etl_id_c"+
				    	",study_name) values("+
				    	"'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
				    	","+pubmedRepository+
				    	","+this.loadDescriptionUI.getPubMedAccession()+ //PubMed id
				    	",'Publication Web Link'"+
				    	",'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
				    	",'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
		    			")"
			    	);
			    	int bio_file_content_id=0;
			    	boolean hasId=true;
			    	rs = stmt.executeQuery("SELECT bio_file_content_id from bio_content where file_name='"+this.loadDescriptionUI.getAccession()+"' and file_type='Publication Web Link'");
			    	if(rs.next()){
			    		bio_file_content_id=rs.getInt("bio_file_content_id");
			    	}
			    	else{
			    		hasId=false;
			    	}
			    	if(hasId){
				    	rs = stmt.executeQuery(
					    	"insert into bio_content_reference"+
					    	"(bio_content_id"+
					    	",bio_data_id"+
					    	",content_reference_type"+
					    	",etl_id_c) values("+
					    	Integer.toString(bio_file_content_id)+//bio file content id from biomart.bio_content
					    	","+Integer.toString(bio_experiment_id)+//bio_experiment_id from bio_experiment
					    	",'Publication Web Link'"+
					    	",'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
					    	")"
				    	);	
			    	}
		    	}
		    	
		    	//taxonomy
		    	if(this.loadDescriptionUI.getOrganism().compareTo("")!=0){
		    		rs=stmt.executeQuery("select bio_taxonomy_id from bio_taxonomy where taxon_name='"+this.loadDescriptionUI.getOrganism()+"'");
			    	if(rs.next()){
			    		rs=stmt.executeQuery("insert into biomart.bio_data_taxonomy values("+Integer.toString(rs.getInt("bio_taxonomy_id"))+", "+Integer.toString(bio_experiment_id)+", '"+this.loadDescriptionUI.getAccession().toString()+"')");
			    	}
		    	}
		    	
		    	con.commit();
		    	con.close();
		    	//tag
		    	con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
				stmt = con.createStatement();
				
				rs=stmt.executeQuery("delete from i2b2_tags where tag='"+this.loadDescriptionUI.getAccession().toUpperCase()+"'");
				
		    	rs=stmt.executeQuery("select max(tag_id) from i2b2_tags");
		    	int max_tag_id;
		    	if(rs.next()){
		    		max_tag_id=rs.getInt("max(tag_id)")+1;
		    	}
		    	else{
		    		max_tag_id=0;
		    	}
		    	rs=stmt.executeQuery("insert into i2b2_tags values("+
			    	Integer.toString(max_tag_id)+
			    	",'"+this.loadDescriptionUI.getTopNode()+"'"+
			    	",'"+this.loadDescriptionUI.getAccession().toUpperCase()+"'"+
			    	",'Trial'"+
			    	",0)"
		    	);
		    	con.commit();
		    	con.close();
		    	this.loadDescriptionUI.displayMessage("Description has been loaded");
		    	
		}catch(SQLException sqle){
			this.loadDescriptionUI.displayMessage("SQL error: "+sqle.getLocalizedMessage());
			sqle.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			// TODO Auto-generated catch block
			this.loadDescriptionUI.displayMessage("Java error: Class not found exception");
			cnfe.printStackTrace();
		}
	}
}
