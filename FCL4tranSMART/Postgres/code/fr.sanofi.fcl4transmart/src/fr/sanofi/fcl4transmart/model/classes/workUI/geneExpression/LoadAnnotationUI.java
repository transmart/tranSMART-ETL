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
package fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression;

import java.util.Vector;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.CheckAnnotationListener;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.LoadAnnotationListener;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class allows the creation of the composite to load platform anottation data
 */
public class LoadAnnotationUI implements WorkItf{
	private Composite scrolledComposite;
	private Composite resultsPart;
	private Text platformId;
	private Text pathField;
	private Text annotationDateField;
	private Text annotationReleaseField;
	private Text annotationTitleField;
	private Shell loadingShell;
	private Shell searchingShell;
	private boolean isLoading;
	private boolean isSearching;
	private Display display;
	private String message;
	private DataTypeItf dataType;
	private Button searchPath;
	private Button loadButton;
	private Button loadAgain;
	public LoadAnnotationUI(DataTypeItf dataType){
		this.dataType=dataType;
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
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		ScrolledComposite scroller=new ScrolledComposite(composite, SWT.H_SCROLL | SWT.V_SCROLL);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		scroller.setLayout(gd);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		this.scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(this.scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		this.scrolledComposite.setLayout(layout);
		scrolledComposite.setLayoutData(new GridData(GridData.FILL_BOTH));

		Composite platformIdPart=new Composite(this.scrolledComposite, SWT.NONE);gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		scroller.setLayout(gd);
		platformIdPart.setLayout(gd);
		this.resultsPart=new Composite(this.scrolledComposite, SWT.NONE);
		this.resultsPart.setLayout(gd);
		
		//platform identifier part definition
		Label platformLabel=new Label(platformIdPart, SWT.NONE);
		platformLabel.setText("Platform identifier to check:");
		this.platformId=new Text(platformIdPart, SWT.BORDER);
		GridData gridData=new GridData();
		gridData.widthHint=100;
		this.platformId.setLayoutData(gridData);
		
		//add a button whith a listener which check if platform annotation has already been loaded
		Button checkButton=new Button(this.scrolledComposite, SWT.PUSH);
		checkButton.setText("OK");
		if(RetrieveData.testDeappConnection()){
			checkButton.addListener(SWT.Selection, new CheckAnnotationListener(this));
			Label dbLabel=new Label(scrolledComposite, SWT.NONE);
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
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
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		loaded.setLayout(gd);
		//Label label=new Label(loaded, SWT.NONE);
		//label.setText("This platform annotation has already been loaded");
		//this.replaceResultsPart(loaded);
		this.addLoadPart(false);
	}
	@SuppressWarnings("unused")
	public void addLoadPart(boolean editable){
		Composite loadPart=new Composite(this.scrolledComposite, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		loadPart.setLayout(gd);
		
		if(editable){
			Label warn=new Label(loadPart, SWT.NONE);
			warn.setText("This platform annotation has not been loaded yet.\nTo load it now, please fill the following form:");
		}else{
			Label warn=new Label(loadPart, SWT.NONE);
			warn.setText("This platform annotation has already been loaded.");
			this.loadAgain=new Button(loadPart, SWT.CHECK);
			loadAgain.setText("Load this platform again");
			loadAgain.addListener(SWT.Selection, new Listener(){
				@Override
				public void handleEvent(Event event) {
					boolean bool=loadAgain.getSelection();
					pathField.setEditable(bool);
					searchPath.setEnabled(bool);
					annotationTitleField.setEditable(bool);
					annotationDateField.setEditable(bool);
					annotationReleaseField.setEditable(bool);
					loadButton.setEnabled(bool);
				}
			});
		}
		
		Composite load=new Composite(loadPart, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=3;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		load.setLayout(gd);
		load.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Label pathLabel=new Label(load, SWT.NONE);
		pathLabel.setText("Annotation file: ");
		this.pathField=new Text(load, SWT.BORDER);
		this.pathField.setEditable(editable);
		GridData gridData=new GridData();
		gridData.widthHint=100;
		this.pathField.setLayoutData(gridData);
		this.searchPath=new Button(load, SWT.PUSH);
		searchPath.setText("Browse");
		searchPath.setEnabled(editable);
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
		
		Label titleLabel=new Label(load, SWT.NONE);
		titleLabel.setText("Title: ");
		this.annotationTitleField=new Text(load, SWT.BORDER);
		this.annotationTitleField.setEditable(editable);
		gridData=new GridData();
		gridData.widthHint=100;
		this.annotationTitleField.setLayoutData(gridData);
		Label lab=new Label(load, SWT.NONE);
		
		Label dateLabel=new Label(load, SWT.NONE);
		dateLabel.setText("Date (optional): ");
		this.annotationDateField=new Text(load, SWT.BORDER);
		this.annotationDateField.setEditable(editable);
		gridData=new GridData();
		gridData.widthHint=100;
		this.annotationDateField.setLayoutData(gridData);
		Label lab2=new Label(load, SWT.NONE);
		lab2.setText("Format: \"yyyy/MM/dd\"");

		Label releaseLabel=new Label(load, SWT.NONE);
		releaseLabel.setText("Release (optional): ");
		this.annotationReleaseField=new Text(load, SWT.BORDER);
		this.annotationReleaseField.setEditable(editable);
		gridData=new GridData();
		gridData.widthHint=100;
		this.annotationReleaseField.setLayoutData(gridData);
		Label lab3=new Label(load, SWT.NONE);
		
		this.loadButton=new Button(load, SWT.PUSH);
		loadButton.setText("Load");
		loadButton.setEnabled(editable);
		if(RetrieveData.testTm_czConnection() && RetrieveData.testTm_lzConnection() && RetrieveData.testDeappConnection()){
			loadButton.addListener(SWT.Selection, new LoadAnnotationListener(this, this.dataType));
		}
		else{
			loadButton.setEnabled(false);
		}
		this.replaceResultsPart(loadPart);
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
	public void openLoadingShell(){
		this.display=WorkPart.display();
		this.isLoading=true;
		this.loadingShell=new Shell(this.display);
		this.loadingShell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.loadingShell.setLayout(gridLayout);
		this.message="";
		ProgressBar pb = new ProgressBar(this.loadingShell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(this.loadingShell, SWT.NONE);
		searching.setText("Loading...");
		this.loadingShell.open();
	}
	public void waitForThread(){
        while(this.isLoading){
        	if (!this.display.readAndDispatch()) {
                this.display.sleep();
              }	
        }
        this.loadingShell.close();
        if(this.message.compareTo("")!=0){
        	this.displayMessage(message);
        }
	}
	public void setIsLoading(boolean bool){
		this.isLoading=bool;
	}
	public void openSearchingShell(){
		this.display=WorkPart.display();
		this.isSearching=true;
		this.message="";
		this.searchingShell=new Shell(this.display);
		this.searchingShell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.searchingShell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(this.searchingShell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(this.searchingShell, SWT.NONE);
		searching.setText("Searching...");
		this.searchingShell.open();
	}
	public void waitForSearchingThread(){
        while(this.isSearching){
        	if (!this.display.readAndDispatch()) {
                this.display.sleep();
              }	
        }
        this.searchingShell.close();
        if(this.message.compareTo("")!=0 && this.message!=null){
        	this.displayMessage(message);
        }
	}
	public void setIsSearching(boolean bool){
		this.isSearching=bool;
	}
	public void setMessage(String message){
		this.message=message;
	}
	@Override
	public boolean canCopy() {
		return false;
	}
	@Override
	public boolean canPaste() {
		return false;
	}
	@Override
	public Vector<Vector<String>> copy() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void paste(Vector<Vector<String>> data) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void mapFromClipboard(Vector<Vector<String>> data) {
		// TODO Auto-generated method stub
		
	}
}
