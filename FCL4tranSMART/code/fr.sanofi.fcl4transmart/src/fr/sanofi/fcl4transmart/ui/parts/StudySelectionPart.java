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
package fr.sanofi.fcl4transmart.ui.parts;

import java.util.Vector;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.e4.ui.di.UIEventTopic;
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
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.core.services.events.IEventBroker;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.StudySelectionController;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;

public class StudySelectionPart {
	private StudySelectionController studySelectionController;
	private ListViewer viewer;
	private Combo studyField;
	public String studyId;
	@Inject private static IEventBroker eventBroker;
	@Inject private Shell shell;
	@Inject private Display display;
	private Composite parent;
	@PostConstruct
	public void createControls(Composite parent) {
		this.parent=parent;
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		parent.setLayout(gd);
		
		this.viewer=new ListViewer(this.parent);
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
		 parent.layout(true, true);
	}
	public void warningMessage(String message){
	    int style = SWT.ICON_WARNING | SWT.OK;
	    MessageBox messageBox = new MessageBox(this.shell, style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public String askWorkspace(){
		String path=PreferencesHandler.getWorkspace();
		if(path.compareTo("")==0){
			DirectoryDialog dialog=new DirectoryDialog(new Shell());
			path=dialog.open();
		}
		PreferencesHandler.setWorkspace(path);
		return path;
	}
	public String askNewWorkspace(){
		DirectoryDialog dialog=new DirectoryDialog(new Shell());
		String path=dialog.open();
		PreferencesHandler.setWorkspace(path);
		return path;
	}
	@Inject
	void eventReceived(@Optional @UIEventTopic("nameChanged/*") StudyItf study) {
		if (study != null) {
			  this.viewer.refresh();
		  }
	} 
	public static void sendNameChanged(StudyItf study){
		eventBroker.send("nameChanged/syncEvent",study);
	}
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
	public void selectLast(){
		this.viewer.getList().select(this.viewer.getList().getItemCount()-1);
		this.updateStudies();
	}
	public void updateStudies(){
		eventBroker.send("studyChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
	}
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
	public boolean confirm(String message){
		return MessageDialog.openConfirm(new Shell(), "Confirm", message);
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
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
}
