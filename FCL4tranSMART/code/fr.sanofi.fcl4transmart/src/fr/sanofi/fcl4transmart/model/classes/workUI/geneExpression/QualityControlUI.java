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
package fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression;

import java.util.HashMap;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.GeneQCController;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class QualityControlUI implements WorkItf{
	private DataTypeItf dataType;
	private Composite body; 
	private Composite scrolledComposite;
	private Text probeField;
	public QualityControlUI(DataTypeItf dataType){
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
		
		this.scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		this.scrolledComposite.setLayout(layout);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.scrolledComposite.setLayoutData(gridData);
		
		//field to indicate probe id
		Composite probePart=new Composite(this.scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=3;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		probePart.setLayout(gd);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		probePart.setLayoutData(gridData);
		
		Label probeLabel=new Label(probePart, SWT.NONE);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		probeLabel.setLayoutData(gridData);
		probeLabel.setText("Probe identifier: ");
		
		this.probeField=new Text(probePart, SWT.BORDER);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.probeField.setLayoutData(gridData);
		
		Button search=new Button(probePart, SWT.PUSH);
		search.setText("Search");
	    search.addListener(SWT.Selection, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		replaceBody(createBody(probeField.getText()));
	    		
	    	} 
    	}); 
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		search.setLayoutData(gridData);
		
		this.body=new Composite(this.scrolledComposite, SWT.NONE);		
		
		Label dbLabel=new Label(this.scrolledComposite, SWT.NONE);
		if(RetrieveData.testDeappConnection()){
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
		}
		else{
			dbLabel.setText("Warning: connection to database is not possible");
			search.setEnabled(false);
		}
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public void replaceBody(Composite body){
		this.body.dispose();
		this.body=body;
		GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
		data.horizontalSpan=1;
		data.verticalSpan=2;
		this.body.setLayoutData(data);		    
		this.scrolledComposite.layout(true, true);	
		this.scrolledComposite.getParent().layout(true, true);
		this.scrolledComposite.setSize(this.scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
	}
	public Composite createBody(String probeId){
		Composite body=new Composite(this.scrolledComposite, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=4;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		body.setLayout(gd);
		if(!FileHandler.getProbes(((GeneExpressionData)this.dataType).getRawFile()).contains(probeId)){
			Label label=new Label(body, SWT.NONE);
			label.setText("This probe identifier does not exist");
			return body;
		}
		GeneQCController controller=new GeneQCController(this.dataType);
		HashMap<String, Double> fileValues=controller.getFileValues(probeId);
		HashMap<String, Double> dbValues=controller.getDbValues(probeId);
		if(dbValues==null || fileValues==null){
			return body;
		}
		
		Label column1=new Label(body, SWT.NONE);
		column1.setText("Sample\t");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column1.setLayoutData(gridData);
		
		Label column2=new Label(body, SWT.NONE);
		column2.setText("Raw data\t");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column2.setLayoutData(gridData);
		
		Label column3=new Label(body, SWT.NONE);
		column3.setText("Database values\t");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column3.setLayoutData(gridData);
		
		Label column4=new Label(body, SWT.NONE);
		column4.setText("Equals");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column4.setLayoutData(gridData);
		
		for(String key: fileValues.keySet()){
			Label label=new Label(body, SWT.NONE);
			label.setText(key+"\t");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			label.setLayoutData(gridData);
			
			Label rawLabel=new Label(body, SWT.NONE);
			rawLabel.setText(fileValues.get(key)+"\t");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			rawLabel.setLayoutData(gridData);
			
			Label dbLabel=new Label(body, SWT.NONE);
			if(dbValues.containsKey(key)){
				dbLabel.setText(dbValues.get(key)+"\t");
			}
			else{
				dbLabel.setText("NO VALUE");
			}
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			dbLabel.setLayoutData(gridData);
			
			Label eqLabel=new Label(body, SWT.NONE);
			if(dbValues.containsKey(key) && fileValues.containsKey(key)){
				if((dbValues.get(key)-fileValues.get(key))<=0.001 && (dbValues.get(key)-fileValues.get(key))>=-0.001){
					eqLabel.setText("OK");
				}
				else{
					eqLabel.setText("FAIL");
				}
			}
			else{
				eqLabel.setText("FAIL");
			}
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			eqLabel.setLayoutData(gridData);
		}
		return body;
	}
}
