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

import java.io.File;
import org.eclipse.jface.dialogs.MessageDialog;
import java.util.Vector;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.RemoveRawFileListener;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SelectClinicalRawFileListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SelectRawFilesUI implements WorkItf{
	private DataTypeItf dataType;
	private Text pathField;
	private Combo fileTypeField;
	private ListViewer viewer;
	public SelectRawFilesUI(DataTypeItf dataType){
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
		
		Composite pathPart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 3;
		pathPart.setLayout(layout);
		Label pathLabel=new Label(pathPart, SWT.NONE);
		pathLabel.setText("Path: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		pathLabel.setLayoutData(gridData);
		this.pathField=new Text(pathPart, SWT.BORDER);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.pathField.setLayoutData(gridData);
		Button browse=new Button(pathPart, SWT.PUSH);
		browse.setText("Browse");
		browse.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				FileDialog fd=new FileDialog(new Shell(), SWT.NONE);
				String text=fd.open();
				if(text!=null){	
					pathField.setText(text);
				}
			}		
		});
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		browse.setLayoutData(gridData);
		
		Composite fileTypePart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 2;
		fileTypePart.setLayout(layout);
		Label fileTypeLabel=new Label(fileTypePart, SWT.NONE);
		fileTypeLabel.setText("Format: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		fileTypeLabel.setLayoutData(gridData);
		this.fileTypeField=new Combo(fileTypePart, SWT.DROP_DOWN | SWT.BORDER );
	    this.fileTypeField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
		this.fileTypeField.add("Tab delimited raw file");
		this.fileTypeField.add("SOFT");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.fileTypeField.setLayoutData(gridData);
		
		Button add=new Button(scrolledComposite, SWT.PUSH);
		add.setText("Add file");
		add.addListener(SWT.Selection, new SelectClinicalRawFileListener(this, this.dataType));
		
		Label filesLabel=new Label(scrolledComposite, SWT.NONE);
		filesLabel.setText("\nRaw data files:");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		filesLabel.setLayoutData(gridData);
		
		Composite filesPart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 2;
		filesPart.setLayout(layout);
		
		this.viewer=new ListViewer(filesPart);
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
		this.viewer.setInput(((ClinicalData)this.dataType).getRawFiles());
		this.displayNames();
		
		Button remove=new Button(filesPart, SWT.PUSH);
		remove.setText("Remove a file");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		remove.setLayoutData(gridData);
		remove.addListener(SWT.Selection, new RemoveRawFileListener(this.dataType, this));

		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public String getPath(){
		return this.pathField.getText();
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public boolean confirm(String message){
		return MessageDialog.openConfirm(new Shell(), "Confirm", message);
	}
	public String getFormat(){
		if(this.fileTypeField.getSelectionIndex()==-1){
			return "";
		}
		return this.fileTypeField.getItem(this.fileTypeField.getSelectionIndex());
	}
	public void displayNames(){
		for(int i=0; i<this.viewer.getList().getItemCount(); i++){
			this.viewer.getList().setItem(i, ((File)this.viewer.getElementAt(i)).getName());
		}
	}
	public void updateViewer(){
		this.viewer.setInput(((ClinicalData)this.dataType).getRawFiles());
		this.displayNames();
	}
	public File getSelectedRemovedFile(){
		if(this.viewer.getList().getSelectionIndex()>=0){
			return ((ClinicalData)this.dataType).getRawFiles().get(this.viewer.getList().getSelectionIndex());
		}
		return null;
	}
}
