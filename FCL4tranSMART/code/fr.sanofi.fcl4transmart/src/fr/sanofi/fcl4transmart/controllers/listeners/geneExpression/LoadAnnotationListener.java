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

import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadAnnotationUI;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class LoadAnnotationListener implements Listener{
	private LoadAnnotationUI loadAnnotationUI;
	public LoadAnnotationListener(LoadAnnotationUI loadAnnotationUI){
		this.loadAnnotationUI=loadAnnotationUI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String jobPath;
		try {  
			//initiate kettle environment
			KettleEnvironment.init(false);
			
			//find the kettle job to initiate the loading
			URL jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation.kjb");
			jobUrl = FileLocator.toFileURL(jobUrl);  
			jobPath = jobUrl.getPath();
			//create a new job from the kettle file
			JobMeta jobMeta = new JobMeta(jobPath, null);
			Job job = new Job(null, jobMeta);		
			
			//find the other files needed for this job and put them in the cache
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/extract_AFFY_annotation_from_file.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/extract_GEO_annotation_from_file.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation_to_lt.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/run_i2b2_load_annotation_deapp.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/extract_annotation_from_file.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
			jobUrl = new URL("platform:/plugin/fr.sanofi.fcl4transmart/jobs_kettle/load_annotation_to_de_gpl_info.ktr");
			jobUrl = FileLocator.toFileURL(jobUrl); 
		
			job.getJobMeta().setParameterValue("DATA_LOCATION", this.loadAnnotationUI.getPathToFile());

			DirectoryDialog dd=new DirectoryDialog(new Shell());
			dd.setText("Choose the sort directory");
			job.getJobMeta().setParameterValue("SORT_DIR", dd.open());
			job.getJobMeta().setParameterValue("DATA_SOURCE", "A");
			//check if gpl id is not empty
			if(this.loadAnnotationUI.getPlatformId()==null){
				int style = SWT.ICON_WARNING | SWT.OK;
			    MessageBox messageBox = new MessageBox(new Shell(), style);
			    messageBox.setMessage("Please provide the platform identifier");
			    messageBox.open();
			    return;
			}
			job.getJobMeta().setParameterValue("GPL_ID", this.loadAnnotationUI.getPlatformId());
			job.getJobMeta().setParameterValue("SKIP_ROWS","1");
			job.getJobMeta().setParameterValue("GENE_ID","4");
			job.getJobMeta().setParameterValue("GENE_SYMBOL_COL","3");
			job.getJobMeta().setParameterValue("ORGANISM_COL","5");
			job.getJobMeta().setParameterValue("PROBE_COL","2");
			if(this.loadAnnotationUI.getAnnotationDate()!=null){
				job.getJobMeta().setParameterValue("ANNOTATION_DATE", this.loadAnnotationUI.getAnnotationDate());
			}
			if(this.loadAnnotationUI.getAnnotationRelease()!=null){
				job.getJobMeta().setParameterValue("ANNOTATION_RELEASE", this.loadAnnotationUI.getAnnotationRelease());
			}
			//check if annotation title is not empty
			if(this.loadAnnotationUI.getAnnotationTitle()==null){
				int style = SWT.ICON_WARNING | SWT.OK;
			    MessageBox messageBox = new MessageBox(new Shell(), style);
			    messageBox.setMessage("Please provide the annotation title");
			    messageBox.open();
			    return;
			}
			job.getJobMeta().setParameterValue("ANNOTATION_TITLE", this.loadAnnotationUI.getAnnotationTitle());			
			job.getJobMeta().setParameterValue("LOAD_TYPE", "I");
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
			job.getJobMeta().setParameterValue("DEAPP_DB_SERVER", PreferencesHandler.getDbServer());
			job.getJobMeta().setParameterValue("DEAPP_DB_NAME", PreferencesHandler.getDbName());
			job.getJobMeta().setParameterValue("DEAPP_DB_PORT", PreferencesHandler.getDbPort());
			job.getJobMeta().setParameterValue("DEAPP_DB_USER", PreferencesHandler.getDeappUser());
			job.getJobMeta().setParameterValue("DEAPP_DB_PWD", PreferencesHandler.getDeappPwd());		
			
			job.start();
			job.waitUntilFinished();
			@SuppressWarnings("unused")
			Result result = job.getResult();
			this.loadAnnotationUI.displayMessage("Platform annotation has been loaded");
			WorkPart.updateSteps();
		} 
		catch (Exception e1) {
			//this.write(e1.getMessage());
			e1.printStackTrace();
		}
	}
}
