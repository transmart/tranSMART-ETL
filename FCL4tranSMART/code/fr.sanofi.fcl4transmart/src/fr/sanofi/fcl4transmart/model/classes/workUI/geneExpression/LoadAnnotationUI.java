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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.CheckAnnotationListener;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.LoadAnnotationListener;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class LoadAnnotationUI implements WorkItf{
	private Composite scrolledComposite;
	private Composite resultsPart;
	private Text platformId;
	private Text pathField;
	private Text annotationDateField;
	private Text annotationReleaseField;
	private Text annotationTitleField;
	public LoadAnnotationUI(StudyItf study){
	}
	@Override
	public Composite createUI(Composite parent){
		//put a new composite in the parent composite, then divide it in two parts, for the platform identifier, and for the results
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
		scroller.setContent(this.scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		this.scrolledComposite.setLayout(layout);

		Composite platformIdPart=new Composite(this.scrolledComposite, SWT.NONE);
		platformIdPart.setLayout(new RowLayout(SWT.HORIZONTAL));
		this.resultsPart=new Composite(this.scrolledComposite, SWT.NONE);
		this.resultsPart.setLayout(new RowLayout(SWT.VERTICAL));
		
		//platform identifier part definition
		Label platformLabel=new Label(platformIdPart, SWT.NONE);
		platformLabel.setText("Platform id");
		this.platformId=new Text(platformIdPart, SWT.BORDER);
		
		//add a button whith a listener which check if platform annotation has already been loaded
		Button checkButton=new Button(this.scrolledComposite, SWT.PUSH);
		checkButton.setText("OK");
		if(RetrieveData.testDeappConnection()){
			checkButton.addListener(SWT.Selection, new CheckAnnotationListener(this));
		}
		else{
			checkButton.setEnabled(false);
			Label warn=new Label(this.scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
		}
		

		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public void displayLoaded(){
		Composite loaded=new Composite(this.scrolledComposite, SWT.NONE);
		loaded.setLayout(new RowLayout());
		Label label=new Label(loaded, SWT.NONE);
		label.setText("This platform annotation has already been loaded");
		this.replaceResultsPart(loaded);
	}
	public void addLoadPart(){
		Composite load=new Composite(this.scrolledComposite, SWT.NONE);
		load.setLayout(new RowLayout(SWT.VERTICAL));
		Composite pathToLoadPart=new Composite(load, SWT.NONE);
		pathToLoadPart.setLayout(new RowLayout());
		Label pathLabel=new Label(pathToLoadPart, SWT.NONE);
		pathLabel.setText("Annotation file: ");
		this.pathField=new Text(pathToLoadPart, SWT.BORDER);
		Button searchPath=new Button(pathToLoadPart, SWT.PUSH);
		searchPath.setText("Browse");
		Listener listener=new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				FileDialog dialog=new FileDialog(new Shell());
				String dialogResult=dialog.open();
				if(dialogResult!=null){
					pathField.setText(dialogResult);
				}
			}
		};
		searchPath.addListener(SWT.Selection, listener);
		
		Composite titlePart=new Composite(load, SWT.NONE);
		titlePart.setLayout(new RowLayout(SWT.HORIZONTAL));
		Label titleLabel=new Label(titlePart, SWT.NONE);
		titleLabel.setText("Title: ");
		this.annotationTitleField=new Text(titlePart, SWT.BORDER);
		
		Composite datePart=new Composite(load, SWT.NONE);
		datePart.setLayout(new RowLayout(SWT.HORIZONTAL));
		Label dateLabel=new Label(datePart, SWT.NONE);
		dateLabel.setText("Date: ");
		this.annotationDateField=new Text(datePart, SWT.BORDER);
		
		Composite releasePart=new Composite(load, SWT.NONE);
		releasePart.setLayout(new RowLayout(SWT.HORIZONTAL));
		Label releaseLabel=new Label(releasePart, SWT.NONE);
		releaseLabel.setText("Release: ");
		this.annotationReleaseField=new Text(releasePart, SWT.BORDER);
		
		Button loadButton=new Button(load, SWT.PUSH);
		loadButton.setText("Load");
		if(RetrieveData.testTm_czConnection() && RetrieveData.testTm_lzConnection() && RetrieveData.testDeappConnection()){
			loadButton.addListener(SWT.Selection, new LoadAnnotationListener(this));
			Label dbLabel=new Label(scrolledComposite, SWT.NONE);
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
		}
		else{
			loadButton.setEnabled(false);
			Label warn=new Label(scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
		}
		this.replaceResultsPart(load);
	}
	public void replaceResultsPart(Composite resultsPart){
		this.resultsPart.dispose();
		this.resultsPart=resultsPart;
		GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
		data.horizontalSpan=1;
		data.verticalSpan=3;
		this.resultsPart.setLayoutData(data);		    
		this.scrolledComposite.layout(true, true);	
		this.scrolledComposite.getParent().layout(true, true);
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
	}
	public String getPlatformId(){
		return this.platformId.getText();
	}
	public String getPathToFile(){
		return this.pathField.getText();
	}
	public String getAnnotationDate(){
		return this.annotationDateField.getText();
	}
	public String getAnnotationTitle(){
		return this.annotationTitleField.getText();
	}
	public String getAnnotationRelease(){
		return this.annotationReleaseField.getText();
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
