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
package fr.sanofi.fcl4transmart.model.classes.workUI.description;

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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.description.LoadDescriptionListener;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class allows the creation of the composite to load the study description
 */
public class LoadDescriptionUI implements WorkItf{
	private StudyItf study;
	private Vector<Text> fieldsText;
	private Vector<Text> valuesText;
	private Vector<Button> removes;
	private Vector<Label> labelsFields;
	private Vector<Label> labelsValues;
	private Vector<String> fields;
	private Vector<String> values;
	private Button add;
	private Composite scrolledComposite;
	private Composite body;
	public LoadDescriptionUI(StudyItf study){
		this.study=study;
	}
	@Override
	public Composite createUI(Composite parent){
		this.initiate();
		this.fieldsText=new Vector<Text>();
		this.valuesText=new Vector<Text>();
		this.removes=new Vector<Button>();
		this.labelsFields=new Vector<Label>();
		this.labelsValues=new Vector<Label>();
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
		
		
		for(int i=0; i<fields.size(); i++){
			//feild label and combo
			Label fieldLabel=new Label(this.body, SWT.NONE);
			fieldLabel.setText("Field: ");
			this.labelsFields.add(fieldLabel);
			Text fieldText=new Text(this.body, SWT.BORDER);
		    fieldText.setText(this.fields.elementAt(i)); 
	
		    this.fieldsText.add(fieldText);
	
		    fieldText.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=fieldsText.indexOf(e.getSource());
					fields.setElementAt(fieldsText.elementAt(n).getText(), n);
				}
			});
		    GridData gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			gridData.widthHint=100;
			fieldText.setLayoutData(gridData);
			
			//value label and combo
			Label valueLabel=new Label(this.body, SWT.NONE);
			valueLabel.setText("Value: ");
			this.labelsValues.add(valueLabel);
			Text valueText=new Text(this.body, SWT.BORDER);
			valueText.setText(this.values.elementAt(i));
			this.valuesText.add(valueText);
			valueText.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=valuesText.indexOf(e.getSource());
					values.setElementAt(valuesText.elementAt(n).getText(), n);
				}
			});
			gridData=new GridData();
			gridData.horizontalAlignment=SWT.FILL;
			gridData.grabExcessHorizontalSpace=true;
			gridData.widthHint=100;
			valueText.setLayoutData(gridData);
			
			//button to remove lines
			Button remove=new Button(this.body, SWT.PUSH);
			remove.setText("Remove tag");
			this.removes.add(remove);
			remove.addSelectionListener(new SelectionListener(){
				@Override
				public void widgetSelected(SelectionEvent e) {
					int n=removes.indexOf((Button)e.getSource());
					labelsFields.get(n).dispose();
					labelsFields.removeElementAt(n);
					fieldsText.get(n).dispose();
					fieldsText.removeElementAt(n);
					fields.remove(n);
					labelsValues.get(n).dispose();
					labelsValues.removeElementAt(n);
					valuesText.get(n).dispose();
					valuesText.removeElementAt(n);
					values.remove(n);
					removes.get(n).dispose();
					body.layout(true, true);
					removes.removeElementAt(n);

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
		
		Button add=new Button(scrolledComposite, SWT.PUSH);
		add.setText("Add a tag");
		add.addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				//field label and combo
				Label fieldLabel=new Label(body, SWT.NONE);
				fieldLabel.setText("Field: ");
				labelsFields.add(fieldLabel);
				Text fieldText=new Text(body, SWT.BORDER);
				fieldText.setText("");
		
			    fieldsText.add(fieldText);
			    fields.add("");
			    fieldText.addModifyListener(new ModifyListener(){
					public void modifyText(ModifyEvent e){
						int n=fieldsText.indexOf(e.getSource());
						fields.setElementAt(fieldsText.elementAt(n).getText(), n);
					}
				});
			    GridData gridData = new GridData();
				gridData.horizontalAlignment = SWT.FILL;
				gridData.grabExcessHorizontalSpace = true;
				gridData.widthHint=100;
				fieldText.setLayoutData(gridData);
				
				//value label and combo
				Label valueLabel=new Label(body, SWT.NONE);
				valueLabel.setText("Value: ");
				labelsValues.add(valueLabel);
				Text valueText=new Text(body, SWT.BORDER);
				
				valueText.setText("");
				valuesText.add(valueText);
				values.add("");
				valueText.addModifyListener(new ModifyListener(){
					public void modifyText(ModifyEvent e){
						int n=valuesText.indexOf(e.getSource());
						values.setElementAt(valuesText.elementAt(n).getText(), n);
					}
				});
				gridData=new GridData();
				gridData.horizontalAlignment=SWT.FILL;
				gridData.grabExcessHorizontalSpace=true;
				gridData.widthHint=100;
				valueText.setLayoutData(gridData);
				
				//button to remove lines
				Button remove=new Button(body, SWT.PUSH);
				remove.setText("Remove tag");
				removes.add(remove);
				remove.addSelectionListener(new SelectionListener(){
					@Override
					public void widgetSelected(SelectionEvent e) {
						int n=removes.indexOf((Button)e.getSource());
						labelsFields.get(n).dispose();
						labelsFields.removeElementAt(n);
						fieldsText.get(n).dispose();
						fieldsText.removeElementAt(n);
						fields.remove(n);
						labelsValues.get(n).dispose();
						labelsValues.removeElementAt(n);
						valuesText.get(n).dispose();
						valuesText.removeElementAt(n);
						values.remove(n);
						removes.get(n).dispose();
						body.layout(true, true);
						removes.removeElementAt(n);

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
		ok.addListener(SWT.Selection, new LoadDescriptionListener(this));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	private void initiate(){
		this.fields=new Vector<String>();
		this.values=new Vector<String>();
		if(this.study.getTopNode()!=null && this.study.getTopNode().compareTo("")!=0){
			Vector<Vector<String>> tags=RetrieveData.getTags(this.study.getTopNode());
			this.fields=tags.get(0);
			this.values=tags.get(1);
		}
	}
	public Vector<String> getFields(){
		return this.fields;
	}
	public Vector<String> getValues(){
		return this.values;
	}
	public StudyItf getStudy(){
		return this.study;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	@Override
	public boolean canCopy() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean canPaste() {
		// TODO Auto-generated method stub
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