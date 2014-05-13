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
package fr.sanofi.fcl4transmart.model.classes.workUI.description;

import java.util.Vector;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.description.LoadDescriptionListener;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class LoadDescriptionUI implements WorkItf{
	private StudyItf study;
	private Text titleField;
	private Text descriptionField;
	private Text designField;
	private Text ownerField;
	private Text accessTypeField;
	private Text pubmedField;
	private Text institutionField;
	private Text countryField;
	private Text phaseField;
	private Text numberField;
	private Combo organismField;
	private String title;
	private String description;
	private String design;
	private String owner;
	private String institution;
	private String country;
	private String accessType;
	private String phase;
	private String number; 
	private Vector<String> taxonomy;
	private String organism;
	private String pubmed;
	private boolean testBiomart;
	private boolean testMetadata;
	private boolean isSearching;
	public LoadDescriptionUI(StudyItf study){
		this.study=study;
	}
	@Override
	public Composite createUI(Composite parent){
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
		this.isSearching=true;
		new Thread(){
			public void run() {
				title=RetrieveData.retrieveTitle(study.toString());
				if(title==null || title.compareTo("")==0){
					if(study.getTopNode()!= null && study.getTopNode().compareTo("")!=0){
						title=study.getTopNode().split("\\\\", -1)[study.getTopNode().split("\\\\", -1).length-2];
					}
				}
				description=RetrieveData.retrieveDescription(study.toString());
				design=RetrieveData.retrieveDesign(study.toString());
				owner=RetrieveData.retrieveOwner(study.toString());
				institution=RetrieveData.retrieveInstitution(study.toString());
				country=RetrieveData.retrieveCountry(study.toString());
				accessType=RetrieveData.retrieveAccessType(study.toString());
				phase=RetrieveData.retrievePhase(study.toString());
				number=RetrieveData.retrieveNumber(study.toString());
				organism=RetrieveData.retrieveOrganism(study.toString());
				taxonomy=RetrieveData.getTaxononomy();
				pubmed=RetrieveData.retrievePubmed(study.toString());
				testBiomart=RetrieveData.testBiomartConnection();
				testMetadata=RetrieveData.testMetadataConnection();
				isSearching=false;
			}
        }.start();
        Display display=WorkPart.display();
        while(this.isSearching){
        	if (!display.readAndDispatch()) {
                display.sleep();
              }	
        }
		shell.close();
		Composite composite=new Composite(parent, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		
		composite.setLayout(gd);
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		ScrolledComposite scroller=new ScrolledComposite(composite, SWT.H_SCROLL | SWT.V_SCROLL);
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		scroller.setLayout(gd);
		//scroller.setLayout(new FillLayout(SWT.HORIZONTAL));
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		scroller.setExpandHorizontal(true);
		scroller.setMinWidth(200);
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		scrolledComposite.setLayout(gd);
		scrolledComposite.setLayoutData(new GridData(GridData.FILL_BOTH));
		scroller.setContent(scrolledComposite); 
		
		Composite fieldsPart=new Composite(scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		fieldsPart.setLayout(gd);
		fieldsPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Label titleLabel=new Label(fieldsPart, SWT.NONE);
		titleLabel.setText("Title: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		titleLabel.setLayoutData(gridData);
		this.titleField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.titleField.setText(this.title);
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
		this.descriptionField=new Text(fieldsPart, SWT.MULTI | SWT.BORDER | SWT.WRAP | SWT.V_SCROLL);
		this.descriptionField.setText(this.description);
		gridData = new GridData();
		gridData.heightHint=75;
		gridData.widthHint=150;
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.descriptionField.setLayoutData(gridData);
		
		Label designLabel=new Label(fieldsPart, SWT.NONE);
		designLabel.setText("Design Factors: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		designLabel.setData(gridData);
		this.designField=new Text(fieldsPart, SWT.BORDER | SWT.WRAP);
		this.designField.setText(this.design);
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
		this.ownerField.setText(this.owner);
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
		this.institutionField.setText(this.institution);
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
		this.countryField.setText(this.country);
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
		this.accessTypeField.setText(this.accessType);
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
		this.phaseField.setText(this.phase);
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
		this.numberField.setText(this.number);
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
	    for(String s: this.taxonomy){
	    	this.organismField.add(s);
	    }
	    this.organismField.setText(this.organism);
	    gridData = new GridData();
	    gridData.widthHint=150;
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.organismField.setLayoutData(gridData);
		
		Label pubmedLabel=new Label(fieldsPart, SWT.NONE);
		pubmedLabel.setText("Pubmed identifier: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		pubmedLabel.setLayoutData(gridData);
		this.pubmedField=new Text(fieldsPart, SWT.BORDER);
		this.pubmedField.setText(this.pubmed);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.pubmedField.setLayoutData(gridData);
		
		Button load=new Button(scrolledComposite, SWT.PUSH);
		load.setText("Load");
		if(this.testBiomart && this.testMetadata){
			if(this.study.getTopNode()==null || this.study.getTopNode().compareTo("")==0){
				load.setEnabled(false);
				Label warn=new Label(scrolledComposite, SWT.NONE);
				warn.setText("The study node has to be defined first");
				gridData = new GridData();
				gridData.horizontalAlignment = SWT.FILL;
				gridData.grabExcessHorizontalSpace = true;
				warn.setLayoutData(gridData);
			}
			else{
				load.addListener(SWT.Selection, new LoadDescriptionListener(this));
				Label dbLabel=new Label(scrolledComposite, SWT.NONE);
				dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
				gridData = new GridData();
				gridData.horizontalAlignment = SWT.FILL;
				gridData.grabExcessHorizontalSpace = true;
				dbLabel.setLayoutData(gridData);
			}
		}
		else{
			load.setEnabled(false);
			Label warn=new Label(scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			warn.setLayoutData(gridData);
		}
		gridData = new GridData();
		gridData.widthHint=45;
		load.setLayoutData(gridData);
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	
	public String getTitle(){
		//return this.titleField.getText();
		return this.titleField.getText().replaceAll("'", "''");
	}
	public String getDescription(){
		return this.descriptionField.getText().replaceAll("'", "''");
	}
	public String getDesign(){
		return this.designField.getText().replaceAll("'", "''");
	}
	public String getOwner(){
		return this.ownerField.getText().replaceAll("'", "''");
	}
	public String getInstitution(){
		return this.institutionField.getText().replaceAll("'", "''");
	}
	public String getAccessType(){
		return this.accessTypeField.getText().replaceAll("'", "''");
	}
	public String getPubMedAccession(){
		return this.pubmedField.getText().replaceAll("'", "''");
	}
	public String getCountry(){
		return this.countryField.getText().replaceAll("'", "''");
	}
	public String getPhase(){
		return this.phaseField.getText().replaceAll("'", "''");
	}
	public String getNumber(){
		return this.numberField.getText().replaceAll("'", "''");
	}
	public String getAccession(){
		return this.study.toString().replaceAll("'", "''");
	}
	public String getOrganism(){
		if(this.organismField.getSelectionIndex()!=-1){
			return this.organismField.getItem(this.organismField.getSelectionIndex()).replaceAll("'", "''");
		}
		return "";
	}
	public String getTopNode(){
		return this.study.getTopNode().replaceAll("'", "\\\\'");
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
