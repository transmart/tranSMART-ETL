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
/**
 *This class allows the creation of the composite to set new terms for clinical data
 */
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
	private Button numerical;
	private Combo mapping;
	private boolean mappingCreated;
	private Composite columnPart;
	public SetTermsUI(DataTypeItf dataType){
		this.selectedFullName="";
		this.dataType=dataType;
		this.mappingCreated=false;
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
		
		columnPart=new Composite(this.scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=4;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
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
		gridData.widthHint=100;
		this.columnsField.setLayoutData(gridData);
		
	    this.columnsField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
	    /*this.columnsField.addListener(SWT.Selection, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		replaceBody(createBody(columnsField.getText()));
	    		
	    	} 
    	});*/ 
	    numerical=new Button(columnPart, SWT.CHECK);
	    numerical.setText("Numerical");
	    
	    Button search=new Button(columnPart, SWT.PUSH);
	    search.setText("Search");
	    search.addListener(SWT.Selection, new Listener(){
			@Override
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
	public void createMappingZone(){
		Label mappingLabel=new Label(columnPart, SWT.NONE);
	    mappingLabel.setText("Copy mapping from column");
	    
	    this.mapping=new Combo(columnPart, SWT.DROP_DOWN | SWT.BOLD | SWT.WRAP);
	    GridData gridData=new GridData();
	    gridData.horizontalAlignment=SWT.FILL;
	    gridData.grabExcessHorizontalSpace=true;
	    gridData.widthHint=100;
	    this.mapping.setLayoutData(gridData);
	    
	    for(File file: ((ClinicalData)this.dataType).getRawFiles()){
	    	for(String s: FileHandler.getHeaders(file)){
	    		this.mapping.add(file.getName()+" - "+s);
	    	}
	    }
	    Button ok=new Button(columnPart, SWT.PUSH);
	    ok.setText("OK");
	    ok.addListener(SWT.Selection, new Listener(){
	    	public void handleEvent(Event event){
	    		if(mapping.getText().compareTo("")!=0 && columnsField.getText().compareTo("")!=0){
		    		for(int i=0; i<oldTerms.get(mapping.getText()).size(); i++){
	    				int index=oldTerms.get(columnsField.getText()).indexOf(oldTerms.get(mapping.getText()).get(i));
		    			if(index!=-1){
		    				terms.get(columnsField.getText()).set(index, terms.get(mapping.getText()).get(i));
		    			}
		    		}
		    		replaceBody(createBody(columnsField.getText()));
	    		}
	    	}
	    });
	}
	public Composite createBody(String fullName){
		this.selectedFullName=fullName;
		this.termFields=new Vector<Text>();
		Composite body=new Composite(this.scrolledComposite, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		body.setLayout(gd);

		if(fullName.compareTo("")==0) return body;
		
		if(!this.mappingCreated){
			this.createMappingZone();
			this.mappingCreated=true;
		}
		
		Label name=new Label(body, SWT.NONE);
		name.setText("Property: "+fullName);
		
		Composite grid=new Composite(body, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		gd.horizontalSpacing=10;
		gd.verticalSpacing=5;
		grid.setLayout(gd);
		
		boolean allNumerical=true;
		for(int i=0; i<this.oldTerms.get(fullName).size(); i++){
			if(!this.numerical.getSelection()){
				allNumerical=false;
				
				Text oldDataLabel=new Text(grid, SWT.BORDER);
				oldDataLabel.setEditable(false);
				oldDataLabel.setText(this.oldTerms.get(fullName).elementAt(i));
				GridData gridData = new GridData();
				gridData.horizontalAlignment = SWT.FILL;
				gridData.grabExcessHorizontalSpace = true;
				gridData.widthHint=150;
				oldDataLabel.setLayoutData(gridData);
				
				Text newDataField=new Text(grid, SWT.BORDER);
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
				gridData.widthHint=150;
				this.termFields.elementAt(i).setLayoutData(gridData);
			}
			else{
				try{
					Double.valueOf(this.oldTerms.get(fullName).elementAt(i));
					this.termFields.add(null);
				}
				catch(Exception e){
					allNumerical=false;
					Label oldDataLabel=new Label(grid, SWT.NONE);
					oldDataLabel.setText(this.oldTerms.get(fullName).elementAt(i));
					GridData gridData = new GridData();
					gridData.horizontalAlignment = SWT.FILL;
					gridData.grabExcessHorizontalSpace = true;
					oldDataLabel.setLayoutData(gridData);
					
					Text newDataField=new Text(grid, SWT.BORDER);
					newDataField.setText(this.terms.get(fullName).elementAt(i));
					if(newDataField.getText().compareTo("")==0){
						newDataField.setText(".");
						terms.get(selectedFullName).setElementAt(".",i);
					}
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
					gridData.widthHint=150;
					this.termFields.elementAt(i).setLayoutData(gridData);
				}
			}
		}
		if(allNumerical){
			Label label=new Label(body, SWT.NONE);
			label.setText("Contains only numerical values.");
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
		File cmf=((ClinicalData)this.dataType).getCMF();
		if(wmf!=null){
			if(cmf!=null){
				for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
					for(String s: FileHandler.getHeaders(rawFile)){
						String fullName=rawFile.getName()+" - "+s;
						this.terms.put(fullName, new Vector<String>());
						this.oldTerms.put(fullName, new Vector<String>());
						for(String oldData: FileHandler.getTerms(rawFile, s)){
							if(oldData.compareTo("")!=0){
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
			}

		}
		else{
			if(cmf!=null){
				for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
					for(String s: FileHandler.getHeaders(rawFile)){
						String fullName=rawFile.getName()+" - "+s;
						this.terms.put(fullName, new Vector<String>());
						this.oldTerms.put(fullName, new Vector<String>());
						for(String oldData: FileHandler.getTerms(rawFile, s)){
							if(oldData.compareTo("")!=0){
								this.terms.get(fullName).add("");
								this.oldTerms.get(fullName).add(oldData);
							}
						}
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
	@Override
	public boolean canCopy() {
		if(this.selectedFullName==null || this.selectedFullName.compareTo("")==0){
			return false;
		}
		return true;
	}
	@Override
	public boolean canPaste() {
		if(this.selectedFullName==null || this.selectedFullName.compareTo("")==0){
			return false;
		}
		return true;
	}
	@Override
	public Vector<Vector<String>> copy() {
		if(this.selectedFullName==null){
			return null;
		}
		Vector<Vector<String>> data=new Vector<Vector<String>>();
		if(this.oldTerms.get(this.selectedFullName)!=null){
			data.add(this.oldTerms.get(this.selectedFullName));
		}
		if(this.terms.get(this.selectedFullName)!=null){
			data.add(this.terms.get(this.selectedFullName));
		}
		return data;
	}
	@Override
	public void paste(Vector<Vector<String>> data) {
		if(this.selectedFullName==null)	return;
		if(data.size()<1) return;
		Vector<String> v=this.terms.get(selectedFullName);
		int l=v.size();
		if(data.get(0).size()<l) l=data.get(0).size();
		for(int i=0; i<l; i++){
			this.terms.get(selectedFullName).set(i, data.get(0).get(i));
			this.termFields.get(i).setText(data.get(0).get(i));
		}
		
	}
	@Override
	public void mapFromClipboard(Vector<Vector<String>> data) {
		if(this.selectedFullName==null)	return;
		if(data.size()<2) return;
		for(int i=0; i<data.get(0).size(); i++){
			int index=this.oldTerms.get(this.selectedFullName).indexOf(data.get(0).get(i));
			if(index!=-1){
				if(data.get(1).size()>i){
					this.terms.get(selectedFullName).set(index, data.get(1).get(i));
					this.termFields.get(index).setText(this.terms.get(selectedFullName).get(index));
				}
			}
		}
		
	}
}
