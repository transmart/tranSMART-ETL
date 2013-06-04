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
package fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData;


import java.io.File;
import java.util.Vector;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
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

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SelectIdentifiersListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class allows the creation of the composite to set subject identifiers for clinical data
 */
public class SetSubjectsIdUI implements WorkItf{
	private DataTypeItf dataType;
	private Vector<String> subjectIds;
	private Vector<Combo> subjectsFields;
	public SetSubjectsIdUI(DataTypeItf dataType){
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent) {
		this.initiate();

		this.subjectsFields=new Vector<Combo>();
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
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		scrolledComposite.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Vector<File> files=((ClinicalData)this.dataType).getRawFiles();
		for(int i=0; i<files.size(); i++){
			Label title=new Label(scrolledComposite, SWT.NONE);
			title.setText(files.elementAt(i).getName());
			
			Composite fieldsPart=new Composite(scrolledComposite, SWT.NONE);
			GridLayout gridLayout=new GridLayout();
			gridLayout.numColumns=2;
			fieldsPart.setLayout(gridLayout);
			
			Label subjectLabel=new Label(fieldsPart, SWT.NONE);
			subjectLabel.setText("Subject identifiers: ");
			GridData gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			subjectLabel.setLayoutData(gridData);
			Combo subjectField=new Combo(fieldsPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		    subjectField.setText(this.subjectIds.elementAt(i));
		    subjectField.addListener(SWT.KeyDown, new Listener(){ 
			    	public void handleEvent(Event event) { 
			    		event.doit = false; 
			    	} 
		    	}); 

		    this.subjectsFields.add(subjectField);

		    subjectField.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=subjectsFields.indexOf(e.getSource());
					subjectIds.setElementAt(subjectsFields.elementAt(n).getText(), n);
				}
			});
		    gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			gridData.widthHint=100;
			subjectField.setLayoutData(gridData);
			
			for(String s: FileHandler.getHeaders(files.elementAt(i))){
		    	this.subjectsFields.elementAt(i).add(s);
		    }
		}
		
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SelectIdentifiersListener(this, this.dataType));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	private void initiate(){
		this.subjectIds=new Vector<String>();
		File cmf=((ClinicalData)this.dataType).getCMF();
		if(cmf!=null){
			for(File file: ((ClinicalData)this.dataType).getRawFiles()){
				int columnNumber=FileHandler.getNumberForLabel(cmf, "SUBJ_ID", file);
				if(columnNumber!=-1){
					this.subjectIds.add(FileHandler.getColumnByNumber(file, columnNumber));
				}
				else{
					this.subjectIds.add("");
				}
			}
		}
		else{
			for(@SuppressWarnings("unused") File file: ((ClinicalData)this.dataType).getRawFiles()){
				this.subjectIds.add("");
			}
		}
	}
	public Vector<String> getSubjectIds(){
		return this.subjectIds;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
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
