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
import java.util.HashMap;
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
import org.eclipse.swt.widgets.Text;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SetTermsListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
public class SetTermsUI implements WorkItf{
	private Composite body;
	private DataTypeItf dataType;
	private Composite scrolledComposite;
	private Combo columnsField;
	private HashMap<String, Vector<String>> oldTerms;
	private HashMap<String, Vector<String>> terms;
	private Vector<Text> termFields;
	private String selectedFullName;
	public SetTermsUI(DataTypeItf dataType){
		this.selectedFullName="";
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent){
		this.initiate();
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
		
		Composite columnPart=new Composite(this.scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		columnPart.setLayout(gd);
		
		Label columnLabel=new Label(columnPart, SWT.NONE);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		columnLabel.setLayoutData(gridData);
		columnLabel.setText("Column: ");
		
		this.columnsField=new Combo(columnPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.columnsField.setLayoutData(gridData);
		
	    this.columnsField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
	    this.columnsField.addListener(SWT.Selection, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		replaceBody(createBody(columnsField.getText()));
	    		
	    	} 
    	}); 
		for(File file: ((ClinicalData)this.dataType).getRawFiles()){
			for(String s: FileHandler.getHeaders(file)){
				this.columnsField.add(file.getName()+" - "+s);
			}
		}
		
		this.body=new Composite(this.scrolledComposite, SWT.NONE);
		
		this.scrolledComposite.setSize(this.scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public Composite createBody(String fullName){
		this.selectedFullName=fullName;
		this.termFields=new Vector<Text>();
		Composite body=new Composite(this.scrolledComposite, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=2;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		body.setLayout(gd);
		for(int i=0; i<this.oldTerms.get(fullName).size(); i++){
			Label oldDataLabel=new Label(body, SWT.NONE);
			oldDataLabel.setText(this.oldTerms.get(fullName).elementAt(i));
			GridData gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			oldDataLabel.setLayoutData(gridData);
			
			Text newDataField=new Text(body, SWT.BORDER);
			newDataField.setText(this.terms.get(fullName).elementAt(i));
			this.termFields.add(newDataField);
			this.termFields.elementAt(i).addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=termFields.indexOf(e.getSource());
					terms.get(selectedFullName).setElementAt(termFields.elementAt(n).getText(), n);
				}
			});
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			this.termFields.elementAt(i).setLayoutData(gridData);
		}
		
		Button ok=new Button(body, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SetTermsListener(this, this.dataType));
		
		return body;
	}
	public void replaceBody(Composite body){
		this.body.dispose();
		this.body=body;
		GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
		data.horizontalSpan=1;
		data.verticalSpan=2;
		this.body.setLayoutData(data);		    
		this.scrolledComposite.layout(true, true);	
		this.scrolledComposite.getParent().layout(true, true);
		this.scrolledComposite.setSize(this.scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
	}
	public void initiate(){
		this.terms=new HashMap<String, Vector<String>>();
		this.oldTerms=new HashMap<String, Vector<String>>();
		File wmf=((ClinicalData)this.dataType).getWMF();
		if(wmf!=null){
			for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
				for(String s: FileHandler.getHeaders(rawFile)){
					String fullName=rawFile.getName()+" - "+s;
					this.terms.put(fullName, new Vector<String>());
					this.oldTerms.put(fullName, new Vector<String>());
					for(String oldData: FileHandler.getTerms(rawFile, s)){
						this.oldTerms.get(fullName).add(oldData);
						String newTerm=FileHandler.getNewDataValue(wmf, rawFile, s, oldData);
						if(newTerm!=null){
							this.terms.get(fullName).add(newTerm);
						}
						else{
							this.terms.get(fullName).add("");
						}
					}
				}
			}
		}
		else{
			for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
				for(String s: FileHandler.getHeaders(rawFile)){
					String fullName=rawFile.getName()+" - "+s;
					this.terms.put(fullName, new Vector<String>());
					this.oldTerms.put(fullName, new Vector<String>());
					for(String oldData: FileHandler.getTerms(rawFile, s)){
						this.terms.get(fullName).add("");
						this.oldTerms.get(fullName).add(oldData);
					}
				}
			}
		}
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public HashMap<String, Vector<String>> getOldValues(){
		return this.oldTerms;
	}
	public HashMap<String, Vector<String>> getNewValues(){
		return this.terms;
	}
}
