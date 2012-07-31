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
package fr.sanofi.fcl4transmart.model.classes.workUI.description;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.description.LoadDescriptionListener;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class LoadDescriptionUI implements WorkItf{
	private StudyItf study;
	private Text titleField;
	private Text descriptionField;
	private Text designField;
	private Text ownerField;
	private Text accessTypeField;
	private Text pubmedField;
	private Text topNodeField;
	private Text institutionField;
	private Text countryField;
	private Text phaseField;
	private Text numberField;
	private Combo organismField;
	public LoadDescriptionUI(StudyItf study){
		this.study=study;
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

		Composite fieldsPart=new Composite(scrolledComposite, SWT.NONE);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		fieldsPart.setLayout(gridLayout);
		
		Label titleLabel=new Label(fieldsPart, SWT.NONE);
		titleLabel.setText("Title: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		titleLabel.setLayoutData(gridData);
		this.titleField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.titleField.setText(RetrieveData.retrieveTitle(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.titleField.setLayoutData(gridData);
		
		Label descriptionLabel=new Label(fieldsPart, SWT.NONE);
		descriptionLabel.setText("Description: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		descriptionLabel.setLayoutData(gridData);
		Composite multiTextComposite=new Composite(fieldsPart, SWT.NONE);
		multiTextComposite.setLayout(new GridLayout());
		this.descriptionField=new Text(multiTextComposite, SWT.MULTI | SWT.BORDER | SWT.WRAP | SWT.V_SCROLL);
		this.descriptionField.setText(RetrieveData.retrieveDescription(this.study.toString()));
		gridData = new GridData();
		gridData.heightHint=75;
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.descriptionField.setLayoutData(gridData);
		
		Label designLabel=new Label(fieldsPart, SWT.NONE);
		designLabel.setText("Design: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		designLabel.setData(gridData);
		this.designField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.designField.setText(RetrieveData.retrieveDesign(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.designField.setLayoutData(gridData);
		
		Label ownerLabel=new Label(fieldsPart, SWT.NONE);
		ownerLabel.setText("Owner: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		ownerLabel.setLayoutData(gridData);
		this.ownerField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.ownerField.setText(RetrieveData.retrieveOwner(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.ownerField.setLayoutData(gridData);
		
		Label institutionLabel=new Label(fieldsPart, SWT.NONE);
		institutionLabel.setText("Institution: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		institutionLabel.setLayoutData(gridData);
		this.institutionField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.institutionField.setText(RetrieveData.retrieveInstitution(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.institutionField.setLayoutData(gridData);
		
		Label countryLabel=new Label(fieldsPart, SWT.NONE);
		countryLabel.setText("Country: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		countryLabel.setData(gridData);
		this.countryField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.countryField.setText(RetrieveData.retrieveCountry(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.countryField.setLayoutData(gridData);
		

		Label accessTypeLabel=new Label(fieldsPart, SWT.NONE);
		accessTypeLabel.setText("Access type: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		accessTypeLabel.setLayoutData(gridData);
		this.accessTypeField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.accessTypeField.setText(RetrieveData.retrieveAccessType(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.accessTypeField.setLayoutData(gridData);
		
		Label phaseLabel=new Label(fieldsPart, SWT.NONE);
		phaseLabel.setText("Phase: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		phaseLabel.setLayoutData(gridData);
		this.phaseField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.phaseField.setText(RetrieveData.retrievePhase(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.phaseField.setLayoutData(gridData);
		
		Label numberLabel=new Label(fieldsPart, SWT.NONE);
		numberLabel.setText("Number of subjects: ");
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.horizontalAlignment = SWT.FILL;
		numberLabel.setLayoutData(gridData);
		this.numberField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.numberField.setText(RetrieveData.retrieveNumber(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.numberField.setLayoutData(gridData);

		Label organismLabel=new Label(fieldsPart, SWT.NONE);
		organismLabel.setText("Organism: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		organismLabel.setLayoutData(gridData);
		this.organismField=new Combo(fieldsPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
	    this.organismField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
		this.organismField.add("");
	    for(String s: RetrieveData.getTaxononomy()){
	    	this.organismField.add(s);
	    }
	    this.organismField.setText(RetrieveData.retrieveOrganism(this.study.toString()));
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.organismField.setLayoutData(gridData);
		
		Label pubmedLabel=new Label(fieldsPart, SWT.NONE);
		pubmedLabel.setText("Pubmed identifier: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		pubmedLabel.setLayoutData(gridData);
		this.pubmedField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.pubmedField.setText(RetrieveData.retrievePubmed(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.pubmedField.setLayoutData(gridData);
		
		Label topNodeLabel=new Label(fieldsPart, SWT.NONE);
		topNodeLabel.setText("Top node: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		topNodeLabel.setLayoutData(gridData);
		this.topNodeField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.topNodeField.setText(RetrieveData.retrieveTopNode(this.study.toString()));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.topNodeField.setLayoutData(gridData);
		
		Button load=new Button(scrolledComposite, SWT.PUSH);
		load.setText("Load");
		if(RetrieveData.testBiomartConnection() && RetrieveData.testMetadataConnection()){
			load.addListener(SWT.Selection, new LoadDescriptionListener(this));
			Label dbLabel=new Label(scrolledComposite, SWT.NONE);
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
		}
		else{
			load.setEnabled(false);
			Label warn=new Label(scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
		}
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	
	public String getTitle(){
		return this.titleField.getText();
	}
	public String getDescription(){
		return this.descriptionField.getText();
	}
	public String getDesign(){
		return this.designField.getText();
	}
	public String getOwner(){
		return this.ownerField.getText();
	}
	public String getInstitution(){
		return this.institutionField.getText();
	}
	public String getAccessType(){
		return this.accessTypeField.getText();
	}
	public String getPubMedAccession(){
		return this.pubmedField.getText();
	}
	public String getTopNode(){
		return this.topNodeField.getText();
	}
	public String getCountry(){
		return this.countryField.getText();
	}
	public String getPhase(){
		return this.phaseField.getText();
	}
	public String getNumber(){
		return this.numberField.getText();
	}
	public String getAccession(){
		return this.study.toString();
	}
	public String getOrganism(){
		if(this.organismField.getSelectionIndex()!=-1){
			return this.organismField.getItem(this.organismField.getSelectionIndex());
		}
		return "";
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
