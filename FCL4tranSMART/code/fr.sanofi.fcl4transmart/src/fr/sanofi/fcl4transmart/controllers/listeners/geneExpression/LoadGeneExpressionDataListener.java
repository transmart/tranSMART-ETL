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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.Log4jBufferAppender;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadDataUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class LoadGeneExpressionDataListener implements Listener{
	private DataTypeItf dataType;
	private LoadDataUI loadDataUI;
	public LoadGeneExpressionDataListener(LoadDataUI loadDataUI, DataTypeItf dataType){
		this.dataType=dataType;
		this.loadDataUI=loadDataUI;
	}
	@Override
	public void handleEvent(Event event) {
		String jobPath;
		try {

			//initiate kettle environment
			KettleEnvironment.init(false);
			
			//find the kettle job to initiate the loading
			URL jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_gene_expression_data.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl);  
			jobPath = jobUrl.getPath();
			//create a new job from the kettle file
			JobMeta jobMeta = new JobMeta(jobPath, null);
			Job job = new Job(null, jobMeta);		
			
			//find the other files needed for this job and put them in the cache
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/validate_gene_expression_params.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/validate_gene_expression_columns.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/check_gene_expression_filenames.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_all_gene_expression_files_for_study.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/run_i2b2_process_mrna_data.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_subject_sample_map_to_lt.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/get_list_of_gene_expression_filenames.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_gene_expression_one_study.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/set_gene_expression_filename.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/validate_gene_expression_columns.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_gene_expression_data_to_lz.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			
			job.getJobMeta().setParameterValue("DATA_FILE_PREFIX", ((GeneExpressionData)this.dataType).getRawFile().getName());
			String path=this.dataType.getPath().getAbsolutePath();
			job.getJobMeta().setParameterValue("DATA_LOCATION", path);
			job.getJobMeta().setParameterValue("MAP_FILENAME", ((GeneExpressionData)this.dataType).getStsmf().getName());
			job.getJobMeta().setParameterValue("DATA_TYPE","R");
			
			//find jnjfilepivot.jar
			URL url = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/JnJFilePivot.jar");
			url = FileLocator.toFileURL(url);  
			path = url.getFile().substring(1);//omit the first '/' character for windows
			job.getJobMeta().setParameterValue("FilePivot_LOCATION",path);
			
			job.getJobMeta().setParameterValue("LOAD_TYPE", "I");
			job.getJobMeta().setParameterValue("LOG_BASE", "2");
			job.getJobMeta().setParameterValue("SAMPLE_REMAP_FILENAME", "NOSAMPLEREMAP");
			job.getJobMeta().setParameterValue("SAMPLE_SUFFIX", ".rma-Signal");
			job.getJobMeta().setParameterValue("SECURITY_REQUIRED", "N");
			job.getJobMeta().setParameterValue("SOURCE_CD", "STD");

			File sort=new File(this.dataType.getStudy().getPath().getParentFile().getAbsolutePath()+File.separator+".sort");
			if(!sort.exists()){
				FileUtils.forceMkdir(sort);
			}
			path=sort.getAbsolutePath();
			job.getJobMeta().setParameterValue("SORT_DIR", path);
			
			job.getJobMeta().setParameterValue("STUDY_ID", this.dataType.getStudy().toString());
			job.getJobMeta().setParameterValue("TOP_NODE", this.loadDataUI.getTopNode());
			
			//job.getJobMeta().setParameterValue("JAVA_HOME", "/usr/local/jdk1.6.0_31");

			job.getJobMeta().setParameterValue("TM_CZ_DB_SERVER", PreferencesHandler.getDbServer());
			job.getJobMeta().setParameterValue("TM_CZ_DB_NAME", PreferencesHandler.getDbName());
			job.getJobMeta().setParameterValue("TM_CZ_DB_PORT", PreferencesHandler.getDbPort());
			job.getJobMeta().setParameterValue("TM_CZ_DB_USER", PreferencesHandler.getTm_czUser());
			job.getJobMeta().setParameterValue("TM_CZ_DB_PWD", PreferencesHandler.getTm_czPwd());
			job.getJobMeta().setParameterValue("TM_LZ_DB_SERVER",PreferencesHandler.getDbServer());
			job.getJobMeta().setParameterValue("TM_LZ_DB_NAME", PreferencesHandler.getDbName());
			job.getJobMeta().setParameterValue("TM_LZ_DB_PORT", PreferencesHandler.getDbPort());
			job.getJobMeta().setParameterValue("TM_LZ_DB_USER", PreferencesHandler.getTm_lzUser());
			job.getJobMeta().setParameterValue("TM_LZ_DB_PWD", PreferencesHandler.getTm_lzPwd());
			
			job.start();
			job.waitUntilFinished();
			@SuppressWarnings("unused")
			Result result = job.getResult();
			this.loadDataUI.displayMessage("Gene expression data has been loaded");
			
			Log4jBufferAppender appender = CentralLogStore.getAppender();
			String logText = appender.getBuffer(job.getLogChannelId(), false).toString();
			
			Pattern pattern=Pattern.compile(".*Finished job entry \\[run i2b2_process_mrna_data\\].*", Pattern.DOTALL);
			Matcher matcher=pattern.matcher(logText);
			if(matcher.matches()){
				String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
				Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getTm_czUser(), PreferencesHandler.getTm_czPwd());
				Statement stmt = con.createStatement();
				
				//remove rows for this study before adding new ones
				ResultSet rs=stmt.executeQuery("select max(JOB_ID) from CZ_JOB_AUDIT where STEP_DESC='Starting i2b2_process_mrna_data'");
				int jobId;
				if(rs.next()){
					jobId=rs.getInt("max(JOB_ID)");
				}
				else{
					con.close();
					return;
				}
				
				logText+="\nOracle job id:\n"+String.valueOf(jobId);
				con.close();
			}
			
			this.writeLog(logText);
			CentralLogStore.discardLines(job.getLogChannelId(), false);
			WorkPart.updateSteps();
		} 
		catch (Exception e1) {
			//this.write(e1.getMessage());
			e1.printStackTrace();
		}
	}
	
	public void write(String text)
	{
		FileDialog fd=new FileDialog(new Shell());
		fd.setText("Choose a log file");
		String filePath = fd.open();
		try
		{
			FileWriter fw = new FileWriter(filePath, true);
			BufferedWriter output = new BufferedWriter(fw);
			output.write(text);
			output.flush();			
			output.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
			}
	}
	public void writeLog(String text)
	{
		File log=new File(this.dataType.getPath()+File.separator+"kettle.log");
		try
		{
			FileWriter fw = new FileWriter(log);
			BufferedWriter output = new BufferedWriter(fw);
			output.write(text);		
			output.close();
			((GeneExpressionData)this.dataType).setLogFile(log);
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
}
