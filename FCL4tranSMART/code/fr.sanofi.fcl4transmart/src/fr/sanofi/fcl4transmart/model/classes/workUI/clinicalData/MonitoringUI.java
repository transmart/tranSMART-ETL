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
package fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import fr.sanofi.fcl4transmart.controllers.ClinicalMonitoringController;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class MonitoringUI implements WorkItf{
	private DataTypeItf dataType;
	public MonitoringUI(DataTypeItf dataType){
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent){
		Composite composite=new Composite(parent, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		composite.setLayout(gd);
		
		ScrolledComposite scroller=new ScrolledComposite(composite, SWT.H_SCROLL | SWT.V_SCROLL);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		
		if(!(RetrieveData.testTm_czConnection() && RetrieveData.testTm_lzConnection())){
			Label label=new Label(scrolledComposite, SWT.NONE);
			label.setText("No database connection");	
			scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
			return composite;
		}
		
		Label label=new Label(scrolledComposite, SWT.NONE);
		label.setText(this.createLabelText()+"\n\nYou are connected to database '"+PreferencesHandler.getDb()+"'");		
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		
		return composite;
	}
	public String createLabelText(){
		ClinicalMonitoringController controller=new ClinicalMonitoringController(this.dataType);
		if(!controller.checkLogFileExists()){
			return "No data loaded.\n";
		}
		String labelText="";
		labelText+="Kettle job: ";
		if(controller.kettleSucceed()){
			labelText+="OK\n\n";
			labelText+="Stored procedure: ";
			String procedure=controller.proceduresError();
			if(procedure.compareTo("")==0){
				labelText+="OK\n";
			}
			else{
				labelText+="FAILED\n"+procedure;
			}
		}
		else{
			labelText+="FAILED\n"+
					"See log file for more details\n\n";
		}
		return labelText;
	}
}
