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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.Log4jBufferAppender;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import fr.sanofi.fcl4transmart.controllers.StudySelectionController;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadAnnotationUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the platform annotation loading 
 */	
public class LoadAnnotationListener implements Listener{
	private LoadAnnotationUI loadAnnotationUI;
	private String pathToFile;
	private String filename;
	private String platformId;
	private String annotationDate;
	private String annotationRelease;
	private String annotationTitle;
	private DataTypeItf dataType;
	public LoadAnnotationListener(LoadAnnotationUI loadAnnotationUI, DataTypeItf dataType){
		this.loadAnnotationUI=loadAnnotationUI;
		this.dataType=dataType;
	}
	/**
	 *Loads the annotation:
	 *-initiate Kettle environment
	 *-Find Kettle files
	 *-Set Kettle parameters
	 *-Calls the Kettle job
	 *-Save the log file
	 */	
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		if(this.loadAnnotationUI.getPathToFile()==null || this.loadAnnotationUI.getPathToFile().compareTo("")==0){
			loadAnnotationUI.setIsLoading(false);
			this.loadAnnotationUI.displayMessage("Please provide a file path");
			return;
		}
		if(this.loadAnnotationUI.getAnnotationTitle()==null || this.loadAnnotationUI.getAnnotationTitle().compareTo("")==0){
			loadAnnotationUI.setIsLoading(false);
			this.loadAnnotationUI.displayMessage("Please provide an annotation title");
			return;
		}
		if(this.loadAnnotationUI.getAnnotationDate()!=null && this.loadAnnotationUI.getAnnotationDate().compareTo("")!=0){
			try{
				DateFormat df=new SimpleDateFormat("yyyy/MM/dd");
				Date date=(Date)df.parse(this.loadAnnotationUI.getAnnotationDate());
			}catch(ParseException pe){
				this.loadAnnotationUI.displayMessage("The annotation date is not valid");
				return;
			}
		}
		String pattern = Pattern.quote(System.getProperty("file.separator"));
		String[] pathSplited=this.loadAnnotationUI.getPathToFile().split(pattern);
		this.pathToFile="";
		for(int i=0; i<pathSplited.length-2; i++){
			this.pathToFile+=pathSplited[i]+File.separator;
		}
		this.pathToFile+=pathSplited[pathSplited.length-2];
		this.filename=pathSplited[pathSplited.length-1];
		
		this.platformId=this.loadAnnotationUI.getPlatformId();
		this.annotationDate=this.loadAnnotationUI.getAnnotationDate();
		this.annotationRelease=this.loadAnnotationUI.getAnnotationRelease();
		this.annotationTitle=this.loadAnnotationUI.getAnnotationTitle();
		this.loadAnnotationUI.openLoadingShell();
		Thread thread=new Thread(){
			public void run() {
				try {  
					Class.forName("org.postgresql.Driver");
					//initiate kettle environment
					URL kettleUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/lib/pentaho");
					kettleUrl = FileLocator.toFileURL(kettleUrl);  
					System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", kettleUrl.getPath());
					KettleEnvironment.init(false);
					//find the kettle job to initiate the loading
					URL jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation.kjb");
					jobUrl = FileLocator.toFileURL(jobUrl);  
					String jobPath = jobUrl.getPath();
					//create a new job from the kettle file
					JobMeta jobMeta = new JobMeta(jobPath, null);
					Job job = new Job(null, jobMeta);		
					
					//find the other files needed for this job and put them in the cache
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/extract_annotation_from_file.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl); 
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation_to_lt.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl); 
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/run_i2b2_load_annotation_deapp.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl); 
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/extract_annotation_from_file.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl); 
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation_to_de_gpl_info.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl);
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/cz_end_audit.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl);
					jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/cz_start_audit.ktr");
					jobUrl = FileLocator.toFileURL(jobUrl); 
				
					job.getJobMeta().setParameterValue("DATA_LOCATION", pathToFile);
					job.getJobMeta().setParameterValue("SOURCE_FILENAME", filename);
		
					File sort=new File(StudySelectionController.getWorkspace().getAbsoluteFile()+File.separator+".sort");
					if(!sort.exists()){
						FileUtils.forceMkdir(sort);
					}
					job.getJobMeta().setParameterValue("SORT_DIR", sort.getAbsolutePath());
					job.getJobMeta().setParameterValue("DATA_SOURCE", "A");
					//check if gpl id is not empty
					if(platformId==null){
						loadAnnotationUI.setMessage("Please provide the platform identifier");
					    loadAnnotationUI.setIsLoading(false);
					    return;
					}
					job.getJobMeta().setParameterValue("GPL_ID", platformId);
					job.getJobMeta().setParameterValue("SKIP_ROWS","1");
					job.getJobMeta().setParameterValue("GENE_ID_COL","4");
					job.getJobMeta().setParameterValue("GENE_SYMBOL_COL","3");
					job.getJobMeta().setParameterValue("ORGANISM_COL","5");
					job.getJobMeta().setParameterValue("PROBE_COL","2");
					if(annotationDate!=null){
						job.getJobMeta().setParameterValue("ANNOTATION_DATE", annotationDate);
					}
					if(annotationRelease!=null){
						job.getJobMeta().setParameterValue("ANNOTATION_RELEASE", annotationRelease);
					}
					//check if annotation title is not empty
					if(annotationTitle==null){
						loadAnnotationUI.setMessage("Please provide the annotation title");
					    loadAnnotationUI.setIsLoading(false);
					    return;
					}
					job.getJobMeta().setParameterValue("ANNOTATION_TITLE", annotationTitle);			
					job.getJobMeta().setParameterValue("LOAD_TYPE", "I");
					job.getJobMeta().setParameterValue("TM_CZ_SERVER", PreferencesHandler.getDbServer());
					job.getJobMeta().setParameterValue("TM_CZ_NAME", PreferencesHandler.getDbName());
					job.getJobMeta().setParameterValue("TM_CZ_PORT", PreferencesHandler.getDbPort());
					job.getJobMeta().setParameterValue("TM_CZ_USER", PreferencesHandler.getTm_czUser());
					job.getJobMeta().setParameterValue("TM_CZ_PWD", PreferencesHandler.getTm_czPwd());
					job.getJobMeta().setParameterValue("TM_LZ_SERVER",PreferencesHandler.getDbServer());
					job.getJobMeta().setParameterValue("TM_LZ_NAME", PreferencesHandler.getDbName());
					job.getJobMeta().setParameterValue("TM_LZ_PORT", PreferencesHandler.getDbPort());
					job.getJobMeta().setParameterValue("TM_LZ_USER", PreferencesHandler.getTm_lzUser());
					job.getJobMeta().setParameterValue("TM_LZ_PWD", PreferencesHandler.getTm_lzPwd());
					job.getJobMeta().setParameterValue("DEAPP_SERVER", PreferencesHandler.getDbServer());
					job.getJobMeta().setParameterValue("DEAPP_NAME", PreferencesHandler.getDbName());
					job.getJobMeta().setParameterValue("DEAPP_PORT", PreferencesHandler.getDbPort());
					job.getJobMeta().setParameterValue("DEAPP_USER", PreferencesHandler.getDeappUser());
					job.getJobMeta().setParameterValue("DEAPP_PWD", PreferencesHandler.getDeappPwd());		
					
					job.start();
					job.waitUntilFinished(3000000);
					job.interrupt();
					//job.waitUntilFinished(5000000);
					
					@SuppressWarnings("unused")
					Result result = job.getResult();
					
					Log4jBufferAppender appender = CentralLogStore.getAppender();
					String logText = appender.getBuffer(job.getLogChannelId(), false).toString();
					
					Pattern pattern=Pattern.compile(".*run_i2b2_load_annotation_deapp - .*\\[run_i2b2_load_annotation_deapp\\].*", Pattern.DOTALL);
					Matcher matcher=pattern.matcher(logText);
					if(matcher.matches()){
						String connectionString="jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
						Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getTm_czUser(), PreferencesHandler.getTm_czPwd());
						Statement stmt = con.createStatement();
						
						//remove rows for this study before adding new ones
						ResultSet rs=stmt.executeQuery("select max(JOB_ID) from CZ_JOB_AUDIT where STEP_DESC='Starting i2b2_load_annotation_deapp'");
						int jobId;
						if(rs.next()){
							jobId=rs.getInt(1);
						}
						else{
							con.close();
							loadAnnotationUI.setIsLoading(false);
							return;
						}
						
						logText+="\nOracle job id:\n"+String.valueOf(jobId);
						rs=stmt.executeQuery("select job_status from cz_job_master where job_id="+String.valueOf(jobId));
						if(rs.next()){
							if(rs.getString("job_status").compareTo("Running")==0){
								loadAnnotationUI.setMessage("Kettle job time out because the stored procedure is not over. Please check in a while if loading has succeed");
								loadAnnotationUI.setIsLoading(false);
								return;
							}
						}
						rs=stmt.executeQuery("select ERROR_MESSAGE from CZ_JOB_ERROR where JOB_ID="+String.valueOf(jobId));
						String procedureErrors="";
						if(rs.next()){
							procedureErrors=rs.getString("ERROR_MESSAGE");
						}
						con.close();
						if(procedureErrors.compareTo("")==0){
							loadAnnotationUI.setMessage("Platform annotation has been loaded");
						}
						else{
							loadAnnotationUI.setMessage("Error during procedure: "+procedureErrors);
						}
					}
					else{
						loadAnnotationUI.setMessage("Error in Kettle job: see log file");
					}
					
					writeLog(logText);
					CentralLogStore.discardLines(job.getLogChannelId(), false);
					
					//
					loadAnnotationUI.setIsLoading(false);
				} 
				catch (Exception e1) {
					loadAnnotationUI.setMessage("Error: "+e1.getLocalizedMessage());
					loadAnnotationUI.setIsLoading(false);
					//this.write(e1.getMessage());
					e1.printStackTrace();
				}
				loadAnnotationUI.setIsLoading(false);
			}
		};
		thread.start();
		this.loadAnnotationUI.waitForThread();
		WorkPart.updateSteps();
		WorkPart.addFiles(this.dataType);
	}
	
	/**
	 *Write a given string corresponding to Kettle log in a log file
	 */	public void writeLog(String text)
	{
		File log=new File(dataType.getPath()+File.separator+"annotation.kettle.log");
		try
		{
			FileWriter fw = new FileWriter(log);
			BufferedWriter output = new BufferedWriter(fw);
			output.write(text);		
			output.close();
			((GeneExpressionData)dataType).setAnnotationLogFile(log);
		}
		catch(IOException ioe){
			loadAnnotationUI.setMessage("File error: "+ioe.getLocalizedMessage());
			ioe.printStackTrace();
		}
	}
}
