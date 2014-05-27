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
package fr.sanofi.fcl4transmart.ui.parts;

import java.util.Vector;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.core.runtime.preferences.ConfigurationScope;
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.e4.ui.workbench.IWorkbench;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.core.services.events.IEventBroker;
import org.osgi.service.prefs.Preferences;
import fr.sanofi.fcl4transmart.controllers.StudySelectionController;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
/**
 *This class handles the study selection part
 */
public class StudySelectionPart {
	private StudySelectionController studySelectionController;
	private ListViewer viewer;
	private Combo studyField;
	public String studyId;
	@Inject private static IEventBroker eventBroker;
	@Inject private Shell shell;
	@Inject private Display display;
	private Composite parent;
	@Inject private IWorkbench workbench;
	private Shell licenceShell;
	private Preferences generalPref;
	private boolean licenceAccepted;
	
	@PostConstruct
	public void createControls(Composite parent) {
		this.parent=parent;
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		this.parent.setLayout(gd);
		
		this.viewer=new ListViewer(this.parent, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
		this.viewer.getControl().setLayoutData(new GridData(GridData.FILL_BOTH));

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
		this.viewer.setInput(new Vector<StudyItf>());
		this.viewer.getList().addSelectionListener(new SelectionListener(){

			@Override
			public void widgetSelected(SelectionEvent e) {
				updateStudies();

			}

			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
		});

		this.studySelectionController=new StudySelectionController(this);
	}
	public void setList(Vector<StudyItf> studies){
		 this.viewer.setInput(studies);
		 this.viewer.refresh();
		 parent.layout(true, true);
	}
	public void warningMessage(String message){
	    int style = SWT.ICON_WARNING | SWT.OK;
	    MessageBox messageBox = new MessageBox(this.shell, style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	/**
	 *Returns a string representing a workspace path, get from preferences if possible, or from a directory dialog if not. 
	 */
	public String askWorkspace(){
		String path=PreferencesHandler.getWorkspace();
		if(path.compareTo("")==0){
			DirectoryDialog dialog=new DirectoryDialog(new Shell());
			dialog.setText("Choose a workspace directory");
			path=dialog.open();
		}
		PreferencesHandler.setWorkspace(path);
		return path;
	}
	/**
	 *Returns a string representing a workspace path, get from a directory dialog
	 */
	public String askNewWorkspace(){
		String old;
		if(StudySelectionController.getWorkspace()!=null){
			old=StudySelectionController.getWorkspace().getAbsolutePath();
		}else{
			old="";
		}
		DirectoryDialog dialog=new DirectoryDialog(new Shell());
		dialog.setText("Choose a workspace directory");
		dialog.setFilterPath(old);
		String path=dialog.open();
		PreferencesHandler.setWorkspace(path);
		return path;
	}
	/**
	 *Updates the study list if an event indicates that a study name changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("nameChanged/*") StudyItf study) {
		if (study != null) {
			  this.viewer.refresh();
		  }
	} 
	/**
	 *Send an event indicating that a study name changed
	 */
	public static void sendNameChanged(StudyItf study){
		eventBroker.send("nameChanged/syncEvent",study);
	}
	/**
	 *Handles event receiving for:
	 *-a new study
	 *-a new workspace
	 *-a study to remove from workspace
	 *-a study to remove from database
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("newStudy/*") String string) {
		if(string!=null){
			if (string.compareTo("new study")==0) {
				  this.studySelectionController.studyAdded();
			  }
			else if(string.compareTo("new workspace")==0){
				this.studySelectionController.workspaceChanged();
			}
			else if(string.compareTo("remove study database")==0){
				this.studySelectionController.removeStudyDatabase();
			}
			else if(string.compareTo("remove study file")==0){
				this.studySelectionController.removeStudyFile();
			}
		}
	} 
	/**
	 *Select the last study in the list. Used if a new study has been added 
	 */
	public void selectLast(){
		this.viewer.getList().select(this.viewer.getList().getItemCount()-1);
		this.updateStudies();
	}
	/**
	 *Send an event indicating that the selected study changed 
	 */
	public void updateStudies(){
		eventBroker.send("studyChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
	}
	/**
	 *Returns a string representing a study to remove from workspace, asked from a dialog 
	 */
	public String askRemoveFolder(){
		this.studyId=null;
		this.shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    this.shell.setSize(500,600);
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		this.shell.setLayout(gridLayout);
		
		Label label=new Label(this.shell, SWT.NONE);
		label.setText("Choose a study to remove from workspace: ");
		
		
		this.studyField=new Combo(this.shell, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		studyField.setLayoutData(gridData);
		
		for(int i=0; i<this.viewer.getList().getItemCount(); i++){
			this.studyField.add(this.viewer.getList().getItem(i));
		}
	    
		Button ok=new Button(this.shell, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				studyId=studyField.getText();
				shell.close();
			}
		});
		
		Button cancel=new Button(this.shell, SWT.PUSH);
		cancel.setText("Cancel");
		cancel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				studyId=null;
				shell.close();
			}
		});
	    
		this.shell.open();
	    
	    while(!shell.isDisposed()){
	    	if (!this.display.readAndDispatch()) {
	            this.display.sleep();
	          }
	    }
		return studyId;
	}
	/**
	 *Returns a boolean indicating if the user confirm an action, indicated in a string passed as parameter 
	 */
	public boolean confirm(String message){
		return MessageDialog.openConfirm(new Shell(), "Confirm", message);
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	/**
	 *Returns a string representing a study to delete from database, asked from a dialog 
	 */
	public String askRemoveDb(Vector<String> ids){
		this.studyId=null;
		this.shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    this.shell.setSize(500,600);
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		this.shell.setLayout(gridLayout);
		
		Label label=new Label(this.shell, SWT.NONE);
		label.setText("Choose a study to remove from database: ");
		
		
		this.studyField=new Combo(this.shell, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		studyField.setLayoutData(gridData);
		
		for(String id: ids){
			this.studyField.add(id);
		}
	    
		Button ok=new Button(this.shell, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				studyId=studyField.getText();
				shell.close();
			}
		});
		
		Button cancel=new Button(this.shell, SWT.PUSH);
		cancel.setText("Cancel");
		cancel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				studyId=null;
				shell.close();
			}
		});
	    
		this.shell.open();
	    
	    while(!shell.isDisposed()){
	    	if (!this.display.readAndDispatch()) {
	            this.display.sleep();
	          }
	    }
		return studyId;
	}
	/**
	 *Display the application license, and returns a boolean indicating if this license has been accepted or not 
	 */
	public boolean askLicence(){
		Preferences preferences = ConfigurationScope.INSTANCE.getNode("fr.sanofi.fcl4transmart");
		generalPref= preferences.node(".general");
		
		if(generalPref.get("license_accepted", "").compareTo("yes")==0){
			return true;
		}
		
		licenceShell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL);
		licenceShell.setSize(500,450);
		licenceShell.setText("License");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		licenceShell.setLayout(gridLayout);
		
		Label label=new Label(licenceShell, SWT.BOLD);
		label.setText("License Agreement");
		
		Label label2=new Label(licenceShell, SWT.WRAP);
		GridData gd=new GridData();
		gd.widthHint=480;
		label2.setText("Please read the following license agreement. You must accept the terms of this agreement before using this application.");
		label2.setLayoutData(gd);

		Text text=new Text(licenceShell, SWT.BORDER|SWT.V_SCROLL|SWT.H_SCROLL|SWT.WRAP);
		text.setEditable(false);
		text.setLayoutData(new GridData(GridData.FILL_BOTH));
		text.setText("Framework of Curation and Loading for tranSMART - Version 1.1\n"+  
					"Copyright (C) 2012 Sanofi-Aventis Recherche et Développement\n\n"+
					"This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.\n\n"+
					"This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.\n\n"+
					"You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.\n\n"+
					"Additional terms are applicable to FC&L4tranSMART, in accordance with section 7 of the GNU General Public License version 3:\n"+
					"YOU EXPRESSLY UNDERSTAND AND AGREE THAT YOUR USE OF THE SOFTWARE IS AT YOUR SOLE RISK AND THAT THE SOFTWARE IS PROVIDED \" AS IS \" AND \" AS AVAILABLE. \" . THERE IS NO WARRANTY THAT \n "+
					"(I)	YOUR USE OF THE PROGRAM WILL MEET YOUR REQUIREMENTS,\n"+
					"(II)	YOUR USE OF THE PROGRAM WILL BE FREE FROM ERROR,\n"+
					"(III)	ANY INFORMATION OBTAINED BY YOU AS A RESULT OF YOUR USE OF THE PROGRAM WILL BE ACCURATE OR RELIABLE.");
		
		Composite buttons=new Composite(licenceShell, SWT.RIGHT_TO_LEFT);
		gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		buttons.setLayout(gridLayout);
		Button cancel=new Button(buttons, SWT.PUSH);
		cancel.setText("Cancel");
		cancel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				shell.close();
				licenceAccepted=false;
				workbench.close();
			}
		});
		Button accept=new Button(buttons, SWT.PUSH);
		accept.setText("Accept");
		accept.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				generalPref.put("license_accepted", "yes");
				licenceShell.close();
				licenceAccepted=true;
			}
		});
		
		licenceShell.open();
	    while(!licenceShell.isDisposed()){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }
	    }
	    return licenceAccepted;
	}
}
