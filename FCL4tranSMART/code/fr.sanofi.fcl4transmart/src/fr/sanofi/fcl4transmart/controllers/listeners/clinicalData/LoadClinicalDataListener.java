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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.Log4jBufferAppender;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.i18n.GlobalMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.LoadDataUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

import org.eclipse.e4.core.di.extensions.*;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;

public class LoadClinicalDataListener implements Listener{
	private DataTypeItf dataType;
	private LoadDataUI loadDataUI;
	@Inject @Preference(nodePath="fr.sanofi.fcl4transmart") IEclipsePreferences preferences;

	public LoadClinicalDataListener(LoadDataUI loadDataUI, DataTypeItf dataType){
		this.dataType=dataType;
		this.loadDataUI=loadDataUI;
	}
	@Override
	public void handleEvent(Event event) {
		String jobPath;
		try {  
			String[] splited=this.loadDataUI.getTopNode().split("\\\\",3);
			if(splited[0].compareTo("")!=0){
				this.loadDataUI.displayMessage("A top node has to begin by the character '\\'");
				return;
			}
			try{
				Class.forName("oracle.jdbc.driver.OracleDriver");
				String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
				
				Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
				Statement stmt = con.createStatement();
				ResultSet rs=stmt.executeQuery("select * from table_access where c_name='"+splited[1]+"'");
				if(!rs.next()){//have to add a top node
					stmt.executeQuery("insert into table_access("+
							"c_table_cd,"+
							"c_table_name,"+
							"c_protected_access,"+
							"c_hlevel,"+
							"c_fullname,"+
							"c_name,"+
							"c_synonym_cd,"+
							"c_visualattributes,"+
							"c_totalnum,"+
							"c_facttablecolumn,"+
							"c_dimtablename,"+
							"c_columnname,"+
							"c_columndatatype,"+
							"c_operator,"+
							"c_dimcode,"+
							"c_tooltip,"+
							"c_status_cd) values("+
							"'"+splited[1]+"',"+
							"'i2b2',"+
							"'N',"+
							"0,"+
							"'\\"+splited[1]+"\\',"+
							"'"+splited[1]+"',"+
							"'N',"+	
							"'CA',"+
							"0,"+
							"'concept_cd',"+
							"'concept_dimension',"+
							"'concept_path',"+
							"'T',"+
							"'LIKE',"+
							"'\\"+splited[1]+"\\',"+
							"'\\"+splited[1]+"\\',"+
							"'A')"
						);
				}
				con.close();
			}catch(SQLException e){
				e.printStackTrace();
				return;
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}

			//initiate kettle environment
			GlobalMessages.setLocale(EnvUtil.createLocale("en-US"));
			KettleEnvironment.init(false);
			
			//find the kettle job to initiate the loading
			URL jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/create_clinical_data.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl);  
			jobPath = jobUrl.getPath();
			//create a new job from the kettle file

			JobMeta jobMeta = new JobMeta(jobPath, null);
			Job job = new Job(null, jobMeta);		
			
			
			
			//find the other files needed for this job and put them in the cache
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/validate_clinical_data_params.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/get_data_filenames.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_lt_clinical_data.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/map_data_to_std_format.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/run_i2b2_load_clinical_data.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/set_data_filename.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 

			job.getJobMeta().setParameterValue("DATA_LOCATION", this.dataType.getPath().getAbsolutePath());
			job.getJobMeta().setParameterValue("COLUMN_MAP_FILE", ((ClinicalData)this.dataType).getCMF().getName());
			
			File sort=new File(this.dataType.getStudy().getPath().getParentFile().getAbsolutePath()+File.separator+".sort");
			if(!sort.exists()){
				FileUtils.forceMkdir(sort);
			}
			job.getJobMeta().setParameterValue("SORT_DIR", sort.getAbsolutePath());
			job.getJobMeta().setParameterValue("STUDY_ID", this.dataType.getStudy().toString());
			job.getJobMeta().setParameterValue("TOP_NODE", this.loadDataUI.getTopNode());
			if(((ClinicalData)this.dataType).getWMF()!=null){
				job.getJobMeta().setParameterValue("WORD_MAP_FILE", ((ClinicalData)this.dataType).getWMF().getName());
			}
			job.getJobMeta().setParameterValue("LOAD_TYPE", "I");
			job.getJobMeta().setParameterValue("TM_CZ_DB_SERVER", PreferencesHandler.getDbServer());
			job.getJobMeta().setParameterValue("TM_CZ_DB_NAME", PreferencesHandler.getDbName());
			job.getJobMeta().setParameterValue("TM_CZ_DB_PORT", PreferencesHandler.getDbPort());
			job.getJobMeta().setParameterValue("TM_CZ_DB_USER", PreferencesHandler.getTm_czUser());
			job.getJobMeta().setParameterValue("TM_CZ_DB_PWD", PreferencesHandler.getTm_czPwd());
			job.getJobMeta().setParameterValue("TM_LZ_DB_SERVER", PreferencesHandler.getDbServer());
			job.getJobMeta().setParameterValue("TM_LZ_DB_NAME", PreferencesHandler.getDbName());
			job.getJobMeta().setParameterValue("TM_LZ_DB_PORT", PreferencesHandler.getDbPort());
			job.getJobMeta().setParameterValue("TM_LZ_DB_USER", PreferencesHandler.getTm_lzUser());
			job.getJobMeta().setParameterValue("TM_LZ_DB_PWD", PreferencesHandler.getTm_lzPwd());
			
			job.start();
			job.waitUntilFinished();
			@SuppressWarnings("unused")
			Result result = job.getResult();
			Log4jBufferAppender appender = CentralLogStore.getAppender();
			String logText = appender.getBuffer(job.getLogChannelId(), false).toString();
			Pattern pattern=Pattern.compile(".*Finished job entry \\[run i2b2_load_clinical_data\\] \\(result=\\[true\\]\\).*", Pattern.DOTALL);
			Matcher matcher=pattern.matcher(logText);
			if(matcher.matches()){
				String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
				Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getTm_czUser(), PreferencesHandler.getTm_czPwd());
				Statement stmt = con.createStatement();
				
				//remove rows for this study before adding new ones
				ResultSet rs=stmt.executeQuery("select max(JOB_ID) from CZ_JOB_AUDIT where STEP_DESC='Start i2b2_load_clinical_data'");
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
			
		} 
		catch (Exception e1) {
			e1.printStackTrace();
			return;
		}
		this.loadDataUI.displayMessage("Clinical data has been loaded");
		WorkPart.updateSteps();
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
			((ClinicalData)this.dataType).setLogFile(log);
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
}
