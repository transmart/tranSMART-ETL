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

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;

import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.Study;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.StudySelectionPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class StudySelectionController {
	private Vector<StudyItf> studies;
	private StudySelectionPart studySelectionPart;
	private File workspace;
	private static File staticWorkspace;  
	private boolean isStudyDeleted;
	private boolean isSearching;
	private String studyIdentifier;
	public StudySelectionController(StudySelectionPart studySelectionPart){
		this.studySelectionPart=studySelectionPart;
		if(!this.studySelectionPart.askLicence()) return;
		this.studies=new Vector<StudyItf>();
		String workspaceString=this.studySelectionPart.askWorkspace();
		if(workspaceString==null) return;
		this.workspace=new File(workspaceString);
		if(!workspace.exists()){
			this.studySelectionPart.warningMessage("The workspace does not exist.");
			this.workspace=new File(this.studySelectionPart.askNewWorkspace());
		}
		staticWorkspace=this.workspace;
		this.readDirectory();
		this.studySelectionPart.setList(studies);
	}
	public Vector<StudyItf> getStudies(){
		return this.studies;
	}
	/**
	 *Reads the workspace to create studies objects
	 */	
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
	/**
	 *Creates a new study
	 */	
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
	/**
	 *Checks a new workspace availability and calls the readDirectory(� method
	 */	
	public void workspaceChanged(){
		this.studies=new Vector<StudyItf>();
		String workspaceString=this.studySelectionPart.askNewWorkspace();
		if(workspaceString==null){
			return;
		}
		this.workspace=new File(workspaceString);
		while(!workspace.exists()){
			this.studySelectionPart.warningMessage("The workspace does not exist.");
			String path=this.studySelectionPart.askNewWorkspace();
			if(path==null) return;
			this.workspace=new File(path);
		}
		staticWorkspace=this.workspace;
		this.readDirectory();
		this.studySelectionPart.setList(studies);
	}
	/**
	 *Removes a study from the database after asking the identifier
	 */	
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
	/**
	 *Removes a study folder in the workspace after asking the identifier
	 */	
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
	/**
	 *Removes a given study folder
	 */	 
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
	/**
	 *Removes a given study in database
	 */	  	
	public boolean deleteDbStudy(String studyId){
	  		this.studyIdentifier=studyId;
			Shell shell=new Shell();
			shell.setSize(50, 100);
			GridLayout gridLayout=new GridLayout();
			gridLayout.numColumns=1;
			shell.setLayout(gridLayout);
			ProgressBar pb = new ProgressBar(shell, SWT.HORIZONTAL | SWT.INDETERMINATE);
			pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

			Label searching=new Label(shell, SWT.NONE);
			searching.setText("Searching...");
			shell.open();
			isSearching=true;
			new Thread(){
				public void run() {
		  		try{
		  			Class.forName("org.postgresql.Driver");
					String connectionString="jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
					
					Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
					Statement stmt = con.createStatement();
					@SuppressWarnings("unused")
					boolean rs=stmt.execute("delete from de_subject_microarray_data where trial_name='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from de_subject_microarray_logs where trial_name='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from de_subject_microarray_med where trial_name='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from de_subject_sample_mapping where trial_name='"+studyIdentifier.toUpperCase()+"'");
					con.close();
					
					con = DriverManager.getConnection(connectionString, PreferencesHandler.getDemodataUser(), PreferencesHandler.getDemodataPwd());
					stmt = con.createStatement();
					rs=stmt.execute("delete from concept_counts where concept_path in(select concept_path from concept_dimension where sourcesystem_cd='"+studyIdentifier.toUpperCase()+"')");
					rs=stmt.execute("delete from concept_dimension where sourcesystem_cd='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from patient_dimension where patient_num in(select patient_num from patient_trial where trial='"+studyIdentifier.toUpperCase()+"')");
					rs=stmt.execute("delete from patient_trial where trial='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from observation_fact where modifier_cd='"+studyIdentifier.toUpperCase()+"'");
					con.close();
					
					con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
					stmt = con.createStatement();
					rs=stmt.execute("delete from i2b2_tags where tag='"+studyIdentifier.toUpperCase()+"'");
					rs=stmt.execute("delete from i2b2 where sourcesystem_cd='"+studyIdentifier.toUpperCase()+"'");
					con.close();
			  		isStudyDeleted=true;
				}catch(SQLException e){
					e.printStackTrace();
					isStudyDeleted=false;
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					isStudyDeleted=false;
				}
		  		isSearching=false;
				}
		    }.start();
		    Display display=WorkPart.display();
		    while(isSearching){
		    	if (!display.readAndDispatch()) {
		            display.sleep();
		          }	
		    }
		    shell.close();
	  		return this.isStudyDeleted;
	  	}
	  	public static File getWorkspace(){
	  		return staticWorkspace;
	  	}
}
