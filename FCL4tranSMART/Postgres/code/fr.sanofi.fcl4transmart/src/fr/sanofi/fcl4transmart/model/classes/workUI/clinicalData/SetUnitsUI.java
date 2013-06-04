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
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
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
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SetUnitsListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class allows the creation of the composite to set units for clinical data properties
 */
public class SetUnitsUI implements WorkItf{
	private DataTypeItf dataType;
	private Vector<Combo> columnsFields;
	private Vector<String> columns;
	private Vector<Combo> unitsFields;
	private Vector<String> units;
	private Vector<Label> columnsLabel;
	private Vector<Label> unitsLabels;
	private Vector<Button> buttons;
	private Composite body;
	private Vector<String> columnsFromCmf;
	private Vector<String> columnsFromRaw;
	private Composite scrolledComposite;
	public SetUnitsUI(DataTypeItf dataType){
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent){
		this.initiate();
		this.columnsFields=new Vector<Combo>();
		this.unitsFields=new Vector<Combo>();
		this.columnsLabel=new Vector<Label>();
		this.unitsLabels=new Vector<Label>();
		this.buttons=new Vector<Button>();
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
		
		scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		gd = new GridLayout();
		gd.numColumns = 1;
		scrolledComposite.setLayout(gd);
		
		this.body=new Composite(scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=5;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		this.body.setLayout(gd);
		GridData grid=new GridData(SWT.FILL);
		//grid.heightHint=100;
		this.body.setLayoutData(grid);
		
		
		//get headers to display in combos
		this.columnsFromCmf=new Vector<String>();
		this.columnsFromRaw=new Vector<String>();
		columnsFromCmf.add("");
		columnsFromRaw.add("");
		for(File raw:((ClinicalData)this.dataType).getRawFiles()){
			for(String s: FileHandler.getHeadersFromCmf(((ClinicalData)this.dataType).getCMF(), raw)){
				columnsFromCmf.add(raw.getName()+" - "+s);
			}
			for(String s: FileHandler.getHeaders(raw)){
				columnsFromRaw.add(raw.getName()+" - "+s);
			}
		}
		
		for(int i=0; i<columns.size(); i++){
			//value column label and combo
			Label columnLabel=new Label(this.body, SWT.NONE);
			columnLabel.setText("Value column");
			this.columnsLabel.add(columnLabel);
			Combo columnField=new Combo(this.body, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
			for(String s: this.columnsFromCmf){
				columnField.add(s);
			}
		    columnField.setText(this.columns.elementAt(i));
		    columnField.addListener(SWT.KeyDown, new Listener(){ 
			    	public void handleEvent(Event event) { 
			    		event.doit = false; 
			    	} 
		    	}); 
	
		    this.columnsFields.add(columnField);
	
		    columnField.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=columnsFields.indexOf(e.getSource());
					columns.setElementAt(columnsFields.elementAt(n).getText(), n);
				}
			});
		    GridData gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			gridData.widthHint=100;
			columnField.setLayoutData(gridData);
			
			//unit column label and combo
			Label unitLabel=new Label(this.body, SWT.NONE);
			unitLabel.setText("Unit column");
			this.unitsLabels.add(unitLabel);
			Combo unitField=new Combo(this.body, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
			for(String s: this.columnsFromRaw){
				unitField.add(s);
			}
			unitField.setText(this.units.elementAt(i));
			unitField.addListener(SWT.KeyDown, new Listener(){
				public void handleEvent(Event event){
					event.doit=false;
				}
			});
			this.unitsFields.add(unitField);
			unitField.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=unitsFields.indexOf(e.getSource());
					units.setElementAt(unitsFields.elementAt(n).getText(), n);
				}
			});
			gridData=new GridData();
			gridData.horizontalAlignment=SWT.FILL;
			gridData.grabExcessHorizontalSpace=true;
			gridData.widthHint=100;
			unitField.setLayoutData(gridData);
			
			//button to remove lines, except for the first
			if(i==0){
				@SuppressWarnings("unused")
				Label space=new Label(this.body, SWT.NONE);
			}
			else{
				Button remove=new Button(this.body, SWT.PUSH);
				remove.setText("Remove line");
				this.buttons.add(remove);
				remove.addSelectionListener(new SelectionListener(){
					@Override
					public void widgetSelected(SelectionEvent e) {
						int n=buttons.indexOf((Button)e.getSource());
						columnsLabel.get(n+1).dispose();
						columnsLabel.removeElementAt(n+1);
						columnsFields.get(n+1).dispose();
						columnsFields.removeElementAt(n+1);
						columns.remove(n+1);
						unitsLabels.get(n+1).dispose();
						unitsLabels.removeElementAt(n+1);
						unitsFields.get(n+1).dispose();
						unitsFields.removeElementAt(n+1);
						units.remove(n+1);
						buttons.get(n).dispose();
						body.layout(true, true);
						buttons.removeElementAt(n);

						body.setSize(body.computeSize(SWT.DEFAULT, SWT.DEFAULT));
						body.layout(true, true);
						scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
					}
					@Override
					public void widgetDefaultSelected(SelectionEvent e) {
						// TODO Auto-generated method stub
						
					}
				});
			}
		}
		
		Button add=new Button(scrolledComposite, SWT.PUSH);
		add.setText("Add a line");
		add.addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				//value column label and combo
				Label columnLabel=new Label(body, SWT.NONE);
				columnLabel.setText("Value column");
				columnsLabel.add(columnLabel);
				Combo columnField=new Combo(body, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
				for(String s: columnsFromRaw){
					columnField.add(s);
				}
			    columnField.setText("");
			    columnField.addListener(SWT.KeyDown, new Listener(){ 
				    	public void handleEvent(Event event) { 
				    		event.doit = false; 
				    	} 
			    	}); 
		
			    columnsFields.add(columnField);
			    columns.add("");
			    columnField.addModifyListener(new ModifyListener(){
					public void modifyText(ModifyEvent e){
						int n=columnsFields.indexOf(e.getSource());
						columns.setElementAt(columnsFields.elementAt(n).getText(), n);
					}
				});
			    GridData gridData = new GridData();
				gridData.horizontalAlignment = SWT.FILL;
				gridData.grabExcessHorizontalSpace = true;
				gridData.widthHint=100;
				columnField.setLayoutData(gridData);
				
				//unit column label and combo
				Label unitLabel=new Label(body, SWT.NONE);
				unitLabel.setText("Unit column");
				unitsLabels.add(unitLabel);
				Combo unitField=new Combo(body, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
				for(String s: columnsFromRaw){
					unitField.add(s);
				}
				unitField.setText("");
				unitField.addListener(SWT.KeyDown, new Listener(){
					public void handleEvent(Event event){
						event.doit=false;
					}
				});
				unitsFields.add(unitField);
				units.add("");
				unitField.addModifyListener(new ModifyListener(){
					public void modifyText(ModifyEvent e){
						int n=unitsFields.indexOf(e.getSource());
						units.setElementAt(unitsFields.elementAt(n).getText(), n);
					}
				});
				gridData=new GridData();
				gridData.horizontalAlignment=SWT.FILL;
				gridData.grabExcessHorizontalSpace=true;
				gridData.widthHint=100;
				unitField.setLayoutData(gridData);
				
				//button to remove lines, except for the first
				Button remove=new Button(body, SWT.PUSH);
				remove.setText("Remove line");
				buttons.add(remove);
				remove.addSelectionListener(new SelectionListener(){
					@Override
					public void widgetSelected(SelectionEvent e) {
						int n=buttons.indexOf((Button)e.getSource());
						columnsLabel.get(n+1).dispose();
						columnsLabel.removeElementAt(n+1);
						columnsFields.get(n+1).dispose();
						columnsFields.removeElementAt(n+1);
						columns.remove(n+1);
						unitsLabels.get(n+1).dispose();
						unitsLabels.removeElementAt(n+1);
						unitsFields.get(n+1).dispose();
						unitsFields.removeElementAt(n+1);
						units.remove(n+1);
						buttons.get(n).dispose();
						body.layout(true, true);
						buttons.removeElementAt(n);

						body.setSize(body.computeSize(SWT.DEFAULT, SWT.DEFAULT));
						body.layout(true, true);
						scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
					}
					@Override
					public void widgetDefaultSelected(SelectionEvent e) {
						// TODO Auto-generated method stub
						
					}
				});
				body.setSize(body.computeSize(SWT.DEFAULT, SWT.DEFAULT));
				body.layout(true, true);
				scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
				
			}

			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
			
		});
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SetUnitsListener(this, this.dataType));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	private void initiate(){
		Vector<Vector<String>> v=FileHandler.getUnitsLines(((ClinicalData)this.dataType).getCMF());
		this.columns=new Vector<String>();
		this.units=new Vector<String>();
		if(v!=null){
			if(v.size()>0){
				for(Vector<String> vector_line: v){
					for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
						if(rawFile.getName().compareTo(vector_line.get(0))==0){
							try{
								this.units.add(vector_line.get(0)+" - "+FileHandler.getColumnByNumber(rawFile, Integer.parseInt(vector_line.get(1))));
								this.columns.add(vector_line.get(0)+" - "+FileHandler.getColumnByNumber(rawFile, Integer.parseInt(vector_line.get(2))));
							}
							catch(Exception e){
								System.out.println("column number is not an integer");
							}
						}
					}
				}
			}
			else{
				this.columns.add("");
				this.units.add("");
			}
		}
	}
	
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public Vector<String> getColumns(){
		return this.columns;
	}
	public Vector<String> getUnits(){
		return this.units;
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
