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
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SelectOtherIdentifiersListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 * This class allows creating the composite representing the interface for setting site identifiers and visit names
 * Since version 1.2: this class also allows setting observation names
 */
public class SetOtherIdsUI implements WorkItf{
	private DataTypeItf dataType;
	private Vector<String> siteIds;
	private Vector<String> visitNames;
	private Vector<Combo> siteFields;
	private Vector<Combo> visitFields;
	public SetOtherIdsUI(DataTypeItf dataType){
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent) {
		this.initiate();

		this.siteFields=new Vector<Combo>();
		this.visitFields=new Vector<Combo>();
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
		    GridData gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			fieldsPart.setLayoutData(gridData);
			
			//site
			Label siteLabel=new Label(fieldsPart, SWT.NONE);
			siteLabel.setText("Site identifiers: ");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			siteLabel.setLayoutData(gridData);
			Combo siteField=new Combo(fieldsPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		   	siteField.addListener(SWT.KeyDown, new Listener(){ 
		    	public void handleEvent(Event event) { 
		    		event.doit = false; 
		    	} 
	    	}); 
		    siteField.setText(this.siteIds.elementAt(i));
		    this.siteFields.add(siteField);
		    siteField.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=siteFields.indexOf(e.getSource());
					siteIds.setElementAt(siteFields.elementAt(n).getText(), n);
				}
			});
		    gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			gridData.widthHint=100;
			siteField.setLayoutData(gridData);
			
			//visit
			Label visitLabel=new Label(fieldsPart, SWT.NONE);
			visitLabel.setText("Visit names: ");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			visitLabel.setLayoutData(gridData);
			Combo visitField=new Combo(fieldsPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		    visitField.addListener(SWT.KeyDown, new Listener(){ 
		    	public void handleEvent(Event event) { 
		    		event.doit = false; 
		    	} 
	    	}); 
		    visitField.setText(this.visitNames.elementAt(i));
		    this.visitFields.add(visitField);
		    visitField.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=visitFields.indexOf(e.getSource());
					visitNames.setElementAt(visitFields.elementAt(n).getText(), n);
				}
			});
		    gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.widthHint=100;
			gridData.grabExcessHorizontalSpace = true;
			visitField.setLayoutData(gridData);
		    			
			//fill combos for a file
			this.siteFields.elementAt(i).add("");
	    	this.visitFields.elementAt(i).add("");
	    	for(String s: FileHandler.getHeaders(files.elementAt(i))){
		    	this.siteFields.elementAt(i).add(s);
		    	this.visitFields.elementAt(i).add(s);
		    }
		}
		
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SelectOtherIdentifiersListener(this, this.dataType));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	/**
	 * Initiates the vectors containing identifiers from an eventually existing column mapping file
	 */
	private void initiate(){
		this.siteIds=new Vector<String>();
		this.visitNames=new Vector<String>();
		File cmf=((ClinicalData)this.dataType).getCMF();
		if(cmf!=null){
			for(File file: ((ClinicalData)this.dataType).getRawFiles()){
				int columnNumber=FileHandler.getNumberForLabel(cmf, "SITE_ID", file);
				if(columnNumber!=-1){
					this.siteIds.add(FileHandler.getColumnByNumber(file, columnNumber));
				}
				else{
					this.siteIds.add("");
				}
				columnNumber=FileHandler.getNumberForLabel(cmf, "VISIT_NAME", file);
				if(columnNumber!=-1){
					this.visitNames.add(FileHandler.getColumnByNumber(file, columnNumber));
				}
				else{
					this.visitNames.add("");
				}
			}
		}
		else{
			for(@SuppressWarnings("unused") File file: ((ClinicalData)this.dataType).getRawFiles()){
				this.siteIds.add("");
				this.visitNames.add("");
			}
		}
	}
	public Vector<String> getSiteIds(){
		return this.siteIds;
	}
	public Vector<String> getVisitNames(){
		return this.visitNames;
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
