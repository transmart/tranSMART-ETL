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

/*
 Class handling the creation of new studies from folders or by the user, the checking of the correct organization of a study folder in the workspace, and the updating of the study selection part
 */
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import fr.sanofi.fcl4transmart.model.classes.Study;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.StudySelectionPart;
public class StudySelectionController {
	private Vector<StudyItf> studies;
	private StudySelectionPart studySelectionPart;
	private File workspace;
	public StudySelectionController(StudySelectionPart studySelectionPart){
		this.studySelectionPart=studySelectionPart;
		this.studies=new Vector<StudyItf>();
		String workspaceString=this.studySelectionPart.askWorkspace();
		if(workspaceString==null) return;
		this.workspace=new File(workspaceString);
		if(!workspace.exists()){
			this.studySelectionPart.warningMessage("The workspace does not exist.");
			this.studySelectionPart.askNewWorkspace();
		}
		this.readDirectory();
		this.studySelectionPart.setList(studies);
	}
	public Vector<StudyItf> getStudies(){
		return this.studies;
	}
	public void readDirectory(){
		File[] filesInWorkspace=workspace.listFiles();
		for(int i=0; i<filesInWorkspace.length; i++){
			if(filesInWorkspace[i].isDirectory() && filesInWorkspace[i].getName().compareTo(".sort")!=0){
				this.studies.add(new Study(filesInWorkspace[i].getName(),filesInWorkspace[i]));
			}
		}
		
		String message="Missing folders:\n";
		Vector<StudyItf> studiesWithMissingFolders=new Vector<StudyItf>();
		for(StudyItf study:this.studies){
			if(study.getMissingFolders().size()>0){
				studiesWithMissingFolders.add(study);
				message+="\nStudy "+study.toString()+":\n";
				for(String s:study.getMissingFolders()){
					message+=">"+s+"\n";
				}
			}
		}
		if(studiesWithMissingFolders.size()>0){
			for(StudyItf study: studiesWithMissingFolders){
				this.studies.remove(study);
			}
			this.studySelectionPart.warningMessage(message);
		}
	}
	public void studyAdded(){
		File path=new File(this.workspace.getAbsolutePath()+File.separator+"New_study");
		if(path.exists()){
			this.studySelectionPart.warningMessage("A study named 'New_study' already exists.\nPlease modify its name before adding another study.");
			return;
		}
		path.mkdir();
		(new File(path.getAbsoluteFile()+File.separator+"clinical")).mkdir();
		(new File(path.getAbsoluteFile()+File.separator+"description")).mkdir();
		(new File(path.getAbsoluteFile()+File.separator+"gene")).mkdir();
		this.studies.add(new Study("New_study", path));
		this.studySelectionPart.setList(studies);
		this.studySelectionPart.selectLast();
	}
	public void workspaceChanged(){
		this.studies=new Vector<StudyItf>();
		String workspaceString=this.studySelectionPart.askNewWorkspace();
		if(workspaceString==null){
			return;
		}
		this.workspace=new File(workspaceString);
		if(!workspace.exists()){
			this.studySelectionPart.warningMessage("The workspace does not exist.");
			this.studySelectionPart.askNewWorkspace();
		}
		this.readDirectory();
		this.studySelectionPart.setList(studies);
		this.studySelectionPart.updateStudies();
	}
	public void removeStudyDatabase(){
		//check database connection
  		if(RetrieveData.testMetadataConnection() && RetrieveData.testDemodataConnection() && RetrieveData.testDemodataConnection() && RetrieveData.testBiomartConnection()){
  			//ask for vector of study ids
  			Vector<String> ids=RetrieveData.getStudiesIds();
  			//ask for a study id
  			String studyId=this.studySelectionPart.askRemoveDb(ids);
  			if(studyId==null) return;
  			//ask for confirmation
  			if(!this.studySelectionPart.confirm("Are you sure to want to remove the study "+studyId+" of the database?")){
  				return;
  			}
  			
  			//remove study
  			if(this.deleteDbStudy(studyId)){
  				this.studySelectionPart.displayMessage("The study has been deleted");
  			}
  			else{
  				this.studySelectionPart.displayMessage("Error while deleting the study");
  			}
  		}
  		else{
  			this.studySelectionPart.displayMessage("No database connection");
  		}
	}
	public void removeStudyFile(){
		//ask for a study id
		String studyId=this.studySelectionPart.askRemoveFolder();
		if(studyId==null) return;
		StudyItf selectedStudy=null;
		for(StudyItf study: this.studies){
			if(study.toString().compareTo(studyId)==0){
				selectedStudy=study;
			}
		}
		if(selectedStudy==null){
			return;
		}
		
		//ask for confirmation
		if(!this.studySelectionPart.confirm("Are you sure to want to remove study "+selectedStudy.toString()+" from the workspace?")){
			return;
		}
		
		//remove study
		if(this.deleteDir(selectedStudy.getPath())){
			this.studySelectionPart.displayMessage("The study has been deleted");
			this.studies.remove(selectedStudy);
			this.studySelectionPart.setList(studies);
		}
		else{
			this.studySelectionPart.displayMessage("Error while deleting the study");
		}
	}
	  public boolean deleteDir(File dir) {
	        if (dir.isDirectory()) {
	            String[] children = dir.list();
	            for (int i=0; i<children.length; i++) {
	                boolean success = deleteDir(new File(dir, children[i]));
	                if (!success) {
	                    return false;
	                }
	            }
	        }
	        return dir.delete();
	    } 
	  	public boolean deleteDbStudy(String studyId){
	  		try{
				Class.forName("oracle.jdbc.driver.OracleDriver");
				String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
				
				Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
				Statement stmt = con.createStatement();
				@SuppressWarnings("unused")
				ResultSet rs=stmt.executeQuery("delete from de_subject_microarray_data where trial_name='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from de_subject_microarray_logs where trial_name='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from de_subject_microarray_med where trial_name='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from de_subject_sample_mapping where trial_name='"+studyId.toUpperCase()+"'");
				con.close();
				
				con = DriverManager.getConnection(connectionString, PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
				stmt = con.createStatement();
				rs=stmt.executeQuery("delete from concept_counts where concept_path in(select concept_path from concept_dimension where sourcesystem_cd='"+studyId.toUpperCase()+"')");
				rs=stmt.executeQuery("delete from concept_dimension where sourcesystem_cd='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from patient_dimension where patient_num in(select patient_num from patient_trial where trial='"+studyId.toUpperCase()+"')");
				rs=stmt.executeQuery("delete from patient_trial where trial='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from observation_fact where modifier_cd='"+studyId.toUpperCase()+"'");
				con.close();
				
				con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
				stmt = con.createStatement();
				rs=stmt.executeQuery("delete from i2b2_tags where tag='"+studyId.toUpperCase()+"'");
				rs=stmt.executeQuery("delete from i2b2 where sourcesystem_cd='"+studyId.toUpperCase()+"'");
				con.close();
				return true;
			}catch(SQLException e){
				e.printStackTrace();
				return  false;
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
	  	}
}
