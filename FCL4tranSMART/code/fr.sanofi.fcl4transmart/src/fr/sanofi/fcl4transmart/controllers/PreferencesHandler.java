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
package fr.sanofi.fcl4transmart.controllers;

import java.util.Vector;

import javax.inject.Inject;

import org.eclipse.core.runtime.preferences.ConfigurationScope;
import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.core.services.events.IEventBroker;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.osgi.service.prefs.BackingStoreException;
import org.osgi.service.prefs.Preferences;

public class PreferencesHandler {
	private Preferences preferences;
	private Preferences databasesPref;
	private Preferences generalPref;
	private Text saveNameField;
	private Text dbNameField;
	private Text dbServerField;
	private Text dbPortField;
	private Text tm_czUserField;
	private Text tm_czPwdField;
	private Text tm_lzUserField;
	private Text tm_lzPwdField;
	private Text deappUserField;
	private Text deappPwdField;
	private Text metadataUserField;
	private Text metadataPwdField;
	private Text demodataUserField;
	private Text demodataPwdField;
	private Text biomartUserField;
	private Text biomartPwdField;
	private Shell shell;
	private Vector<String> databases;
	@SuppressWarnings("restriction")
	@Inject  private IEventBroker eventBroker;
	private static Preferences staticPreferences;
	private static Preferences staticDbPref;
	private ListViewer viewer;
	private Composite preferencesPart;
	public PreferencesHandler(){
		this.preferences = ConfigurationScope.INSTANCE.getNode("fr.sanofi.fcl4transmart");
		
		PreferencesHandler.staticPreferences=this.preferences;
		this.generalPref= preferences.node(".general");
		
		String[] subPref;
		this.databases=new Vector<String>();
		try {
			subPref=this.preferences.childrenNames();
		} catch (BackingStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		this.databases.add("");
		for(int i=0; i<subPref.length; i++){
			if(subPref[i].compareTo(".general")!=0){
				this.databases.add(subPref[i]);
			}
		}
		if(databases.contains(this.generalPref.get("selectedDb", ""))){
			this.databasesPref=preferences.node(this.generalPref.get("selectedDb", ""));
			PreferencesHandler.staticDbPref=this.databasesPref;
		}
		else{
			this.databasesPref=null;
			PreferencesHandler.staticDbPref=this.generalPref;
		}
	}
	@Execute
	public void execute(Display display) {
			
	    this.shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    this.shell.setSize(500,600);
	    this.shell.setText("Database preferences");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		this.shell.setLayout(gridLayout);
		
	    
	    Composite selectionPart=new Composite(this.shell, SWT.NONE);
	    selectionPart.setLayout(new GridLayout());
	    //fields
	   this.viewer=new ListViewer(selectionPart);

	   this.viewer.setContentProvider(new IStructuredContentProvider(){
			public Object[] getElements(Object inputElement) {
				@SuppressWarnings("rawtypes")
				Vector v = (Vector)inputElement;
				return v.toArray();
			}
			public void dispose() {
				// TODO Auto-generated method stub
			}

			public void inputChanged(Viewer viewer, Object oldInput,
					Object newInput) {
				// TODO Auto-generated method stub
			}
		});	
	   this.viewer.setInput(this.databases);
		if(databases.contains(this.generalPref.get("selectedDb", ""))){
			this.viewer.getList().setSelection(this.databases.indexOf(this.generalPref.get("selectedDb", "")));
		}		
		
		this.viewer.getList().addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				String selected=(String)viewer.getElementAt(viewer.getList().getSelectionIndex());
				generalPref.put("selectedDb", selected);
				databasesPref=preferences.node(selected);
				staticDbPref=databasesPref;				
				try {
					preferences.flush();
					}
				catch (BackingStoreException bse) {
						bse.printStackTrace();
				}
					preferencesPart.dispose();
					preferencesPart=changePrefPart();
					GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
					data.horizontalSpan=1;
					data.verticalSpan=1;
					preferencesPart.setLayoutData(data);		    
					shell.layout(true, true);					
			}
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
			}
		});
		GridData gridData=new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=125;
		this.viewer.getControl().setLayoutData(gridData);
	   
		this.preferencesPart=this.changePrefPart();
	    
	    
		this.shell.open();
	    
	    while(!shell.isDisposed()){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }
	    }
	    eventBroker.send("preferencesChanged/syncEvent", "Preferences changed");
	}
	public static String getDbName(){
		try{
			return PreferencesHandler.staticDbPref.get("db_name", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDb(){
		Preferences general=PreferencesHandler.staticPreferences.node(".general");
		return general.get("selectedDb", "");
	}
	public static String getDbServer(){
		try{
			return PreferencesHandler.staticDbPref.get("db_server", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDbPort(){
		try{
			return PreferencesHandler.staticDbPref.get("db_port", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getTm_czUser(){
		try{
			return PreferencesHandler.staticDbPref.get("tm_cz_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getTm_czPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("tm_cz_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getTm_lzUser(){
		try{
			return PreferencesHandler.staticDbPref.get("tm_lz_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getTm_lzPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("tm_lz_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDeappUser(){
		try{
			return PreferencesHandler.staticDbPref.get("deapp_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDeappPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("deapp_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getMetadataUser(){
		try{
			return PreferencesHandler.staticDbPref.get("metadata_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getMetadataPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("metadata_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDemodataUser(){
		try{
			return PreferencesHandler.staticDbPref.get("demodata_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getDemodataPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("demodata_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getBiomartUser(){
		try{
			return PreferencesHandler.staticDbPref.get("biomart_user", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public static String getBiomartPwd(){
		try{
			return PreferencesHandler.staticDbPref.get("biomart_pwd", "");
		}
		catch(NullPointerException e){
			return "";
		}
	}
	public Composite changePrefPart(){
		Composite prefPart=new Composite(shell, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=2;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		prefPart.setLayout(gd);
	    
		Label saveNameLabel=new Label(prefPart, SWT.NONE);
		saveNameLabel.setText("Save name: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		saveNameLabel.setLayoutData(gridData);
		this.saveNameField=new Text(prefPart, SWT.BORDER);
		this.saveNameField.setText(generalPref.get("selectedDb", ""));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=150;
		saveNameField.setLayoutData(gridData);
	    
		Label dbServerLabel=new Label(prefPart, SWT.NONE);
		dbServerLabel.setText("Database server: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		dbServerLabel.setLayoutData(gridData);
		this.dbServerField=new Text(prefPart, SWT.BORDER);
		if(this.databasesPref!=null){
			this.dbServerField.setText(databasesPref.get("db_server", ""));
		}
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.dbServerField.setLayoutData(gridData);
		   
	    Label dbNameLabel=new Label(prefPart, SWT.NONE);
	    dbNameLabel.setText("Database name: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		dbNameLabel.setLayoutData(gridData);
	    this.dbNameField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.dbNameField.setText(databasesPref.get("db_name", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.dbNameField.setLayoutData(gridData);
	    
	    Label dbPortLabel=new Label(prefPart, SWT.NONE);
	    dbPortLabel.setText("Database port: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		dbPortLabel.setLayoutData(gridData);
	    this.dbPortField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.dbPortField.setText(databasesPref.get("db_port", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.dbPortField.setLayoutData(gridData);
	    
	    Label tm_czUserLabel=new Label(prefPart, SWT.NONE);
	    tm_czUserLabel.setText("TM_CZ user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		tm_czUserLabel.setLayoutData(gridData);
	    this.tm_czUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.tm_czUserField.setText(databasesPref.get("tm_cz_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.tm_czUserField.setLayoutData(gridData);
	    
	    Label tm_czPwdLabel=new Label(prefPart, SWT.NONE);
	    tm_czPwdLabel.setText("TM_CZ pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		tm_czPwdLabel.setLayoutData(gridData);
	    this.tm_czPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.tm_czPwdField.setText(databasesPref.get("tm_cz_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.tm_czPwdField.setLayoutData(gridData);
	    
	    Label tm_lzUserLabel=new Label(prefPart, SWT.NONE);
	    tm_lzUserLabel.setText("TM_LZ user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		tm_lzUserLabel.setLayoutData(gridData);
	    this.tm_lzUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.tm_lzUserField.setText(databasesPref.get("tm_lz_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.tm_lzUserField.setLayoutData(gridData);
	    
	    Label tm_lzPwdLabel=new Label(prefPart, SWT.NONE);
	    tm_lzPwdLabel.setText("TM_LZ pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		tm_lzPwdLabel.setLayoutData(gridData);
	    this.tm_lzPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.tm_lzPwdField.setText(databasesPref.get("tm_lz_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.tm_lzPwdField.setLayoutData(gridData);
	    
	    Label deappUserLabel=new Label(prefPart, SWT.NONE);
	    deappUserLabel.setText("DEAPP user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		deappUserLabel.setLayoutData(gridData);
	    this.deappUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.deappUserField.setText(databasesPref.get("deapp_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.deappUserField.setLayoutData(gridData);
	    
	    Label deappPwdLabel=new Label(prefPart, SWT.NONE);
	    deappPwdLabel.setText("DEAPP pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		deappPwdLabel.setLayoutData(gridData);
	    this.deappPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.deappPwdField.setText(databasesPref.get("deapp_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.deappPwdField.setLayoutData(gridData);
	    
	    Label metadataUserLabel=new Label(prefPart, SWT.NONE);
	    metadataUserLabel.setText("I2B2METADATA user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		metadataUserLabel.setLayoutData(gridData);
	    this.metadataUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.metadataUserField.setText(databasesPref.get("metadata_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.metadataUserField.setLayoutData(gridData);
	    
	    Label metadataPwdLabel=new Label(prefPart, SWT.NONE);
	    metadataPwdLabel.setText("I2B2METADATA pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		metadataPwdLabel.setLayoutData(gridData);
	    this.metadataPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.metadataPwdField.setText(databasesPref.get("metadata_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.metadataPwdField.setLayoutData(gridData);
	    
	    Label demodataUserLabel=new Label(prefPart, SWT.NONE);
	    demodataUserLabel.setText("I2B2DEMODATA user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		demodataUserLabel.setLayoutData(gridData);
	    this.demodataUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.demodataUserField.setText(databasesPref.get("demodata_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.demodataUserField.setLayoutData(gridData);
	    
	    Label demodataPwdLabel=new Label(prefPart, SWT.NONE);
	    demodataPwdLabel.setText("I2B2DEMODATA pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		demodataPwdLabel.setLayoutData(gridData);
	    this.demodataPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.demodataPwdField.setText(databasesPref.get("demodata_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.demodataPwdField.setLayoutData(gridData);
	    
	    Label biomartUserLabel=new Label(prefPart, SWT.NONE);
	    biomartUserLabel.setText("BIOMART user: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		biomartUserLabel.setLayoutData(gridData);
	    this.biomartUserField=new Text(prefPart, SWT.BORDER);
	    if(this.databasesPref!=null){
	    	this.biomartUserField.setText(databasesPref.get("biomart_user", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.biomartUserField.setLayoutData(gridData);

	    Label biomartPwdLabel=new Label(prefPart, SWT.NONE);
	    biomartPwdLabel.setText("BIOMART pasword: ");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		biomartPwdLabel.setLayoutData(gridData);
	    this.biomartPwdField=new Text(prefPart, SWT.BORDER | SWT.PASSWORD);
	    if(this.databasesPref!=null){
	    	this.biomartPwdField.setText(databasesPref.get("biomart_pwd", ""));
	    }
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.biomartPwdField.setLayoutData(gridData);
	    
		Composite buttonPart=new Composite(prefPart, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=3;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		buttonPart.setLayout(gd);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		buttonPart.setLayoutData(gridData);
		
		Button test=new Button(buttonPart, SWT.PUSH);
		test.setText("Test");
		 gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		test.setLayoutData(gridData);
		test.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				if(!RetrieveData.testDemodataConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), demodataUserField.getText(), demodataPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				if(!RetrieveData.testMetadataConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), metadataUserField.getText(), metadataPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				if(!RetrieveData.testDeappConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), deappUserField.getText(), deappPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				if(!RetrieveData.testTm_czConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), tm_czUserField.getText(), tm_czPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				if(!RetrieveData.testTm_lzConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), tm_lzUserField.getText(), tm_lzPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				if(!RetrieveData.testBiomartConnection(dbServerField.getText(), dbNameField.getText(), dbPortField.getText(), biomartUserField.getText(), biomartPwdField.getText())){
					displayMessage("Connection is not possible");
					return;
				}
				displayMessage("Connection OK");
			}
		});
		
	    Button ok=new Button(buttonPart, SWT.PUSH);
	    ok.setText("Save");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		ok.setLayoutData(gridData);
	    ok.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				if(saveNameField.getText().compareTo("")==0){
				    int style = SWT.ICON_WARNING | SWT.OK;
				    MessageBox messageBox = new MessageBox(new Shell(), style);
				    messageBox.setMessage("Please fill the save name");
				    messageBox.open();
				    return;
				}
				if(!databases.contains(saveNameField.getText())){
					databases.add(saveNameField.getText());
				}
				viewer.setInput(databases);
				viewer.getList().setSelection(databases.indexOf(saveNameField.getText()));
				databasesPref=preferences.node(saveNameField.getText());
		
				databasesPref.put("db_server", dbServerField.getText());
				staticDbPref=databasesPref;	
				
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("db_name", dbNameField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("db_port", dbPortField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("tm_cz_user", tm_czUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("tm_cz_pwd", tm_czPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("tm_lz_user", tm_lzUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("tm_lz_pwd", tm_lzPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("deapp_user", deappUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("deapp_pwd", deappPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("metadata_user", metadataUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("metadata_pwd", metadataPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("demodata_user", demodataUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("demodata_pwd", demodataPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("biomart_user", biomartUserField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				
				databasesPref.put("biomart_pwd", biomartPwdField.getText());
				try {
					preferences.flush();
					}
				catch (BackingStoreException e) {
						e.printStackTrace();
				}
				viewer.getList().setSelection(viewer.getList().indexOf(saveNameField.getText()));
			}
	    });
	    
	    Button load=new Button(buttonPart, SWT.PUSH);
	    load.setText("OK");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		load.setLayoutData(gridData);
	    load.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				shell.dispose();
			}
	    	
	    });
	    return prefPart;
	}
	public static void setWorkspace(String path){
		if(path==null) return;
		Preferences general=staticPreferences.node(".general");
		general.put("workspace", path);
		try {
			staticPreferences.flush();
			}
		catch (BackingStoreException e) {
				e.printStackTrace();
		}
	}
	public static String getWorkspace(){
		Preferences general=staticPreferences.node(".general");
		return general.get("workspace", "");
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
