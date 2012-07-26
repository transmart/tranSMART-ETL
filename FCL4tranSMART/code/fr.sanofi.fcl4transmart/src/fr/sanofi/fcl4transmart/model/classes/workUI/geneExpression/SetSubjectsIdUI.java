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
package fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.SetSubjectsIdListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SetSubjectsIdUI implements WorkItf{
	private DataTypeItf dataType;
	private Text appliedText;
	private String appliedString;
	private Vector<Text> valuesFields;
	private Vector<String> values;
	private Vector<Button> checkBoxs;
	private Vector<String> samples;
	public SetSubjectsIdUI(DataTypeItf dataType){
		this.dataType=dataType;
		this.appliedString="";
	}
	@Override
	public Composite createUI(Composite parent){
		this.valuesFields=new Vector<Text>();
		this.checkBoxs=new Vector<Button>();
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
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		
		Composite body=new Composite(scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=3;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		body.setLayout(gd);
		
		Label appliedLabel=new Label(body, SWT.NONE);
		appliedLabel.setText("Value: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		appliedLabel.setLayoutData(gridData);
		
		this.appliedText=new Text(body, SWT.BORDER);
		this.appliedText.setText(this.appliedString);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.appliedText.setLayoutData(gridData);
		this.appliedText.addModifyListener(new ModifyListener(){
			@Override
			public void modifyText(ModifyEvent e) {
				appliedString=appliedText.getText();
			}	
		});
		
		Button apply=new Button(body, SWT.PUSH);
		apply.setText("Apply");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		apply.setLayoutData(gridData);
		apply.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				for(int i=0; i<valuesFields.size(); i++){
					if(checkBoxs.elementAt(i).getSelection()){
						valuesFields.elementAt(i).setText(appliedText.getText());
					}
				}
				
			}
		});
		
		//let a blank line
		for(int i=0; i<3; i++){
			Label label=new Label(body, SWT.NONE);
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			label.setLayoutData(gridData);
		}
		
		Label column1=new Label(body, SWT.NONE);
		column1.setText("Sample\t\t");
		gridData=new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column1.setLayoutData(gridData);
		
		Label column2=new Label(body, SWT.NONE);
		column2.setText("Value\t\t");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column2.setLayoutData(gridData);
		
		Label column3=new Label(body, SWT.NONE);
		column3.setText("");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		column3.setLayoutData(gridData);
		
		for(int i=0; i<this.samples.size(); i++){
			Label valueLabel=new Label(body, SWT.NONE);
			valueLabel.setText(samples.elementAt(i));
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			valueLabel.setLayoutData(gridData);
			
			Text valueText=new Text(body, SWT.BORDER);
			valueText.setText(this.values.elementAt(i));
			this.valuesFields.add(valueText);
			this.valuesFields.elementAt(i).addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=valuesFields.indexOf(e.getSource());
					values.setElementAt(valuesFields.elementAt(n).getText(), n);
				}
			});
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			valueText.setLayoutData(gridData);
			
			Button checkBox=new Button(body, SWT.CHECK);
			
			this.checkBoxs.add(checkBox);
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			checkBox.setLayoutData(gridData);
		}
		
		Button select=new Button(body, SWT.PUSH);
		select.setText("Select all");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		select.setLayoutData(gridData);
		select.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				for(Button b: checkBoxs){
					b.setSelection(true);
				}
			}
		});
		
		Button deselect=new Button(body, SWT.PUSH);
		deselect.setText("Deselect all");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		deselect.setLayoutData(gridData);
		deselect.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				for(Button b: checkBoxs){
					b.setSelection(false);
					
				}
			}
		});
		
		Label l=new Label(body, SWT.NONE);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		l.setLayoutData(gridData);
		
		Button ok=new Button(body, SWT.PUSH);
		ok.setText("Ok");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		ok.setLayoutData(gridData);
		ok.addListener(SWT.Selection, new SetSubjectsIdListener(this.dataType, this));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public void initiate(){
		this.values=new Vector<String>();
		this.samples=FileHandler.getSamplesId(((GeneExpressionData)this.dataType).getRawFile());
		File stsmf=((GeneExpressionData)this.dataType).getStsmf();
		for(@SuppressWarnings("unused") String sample: samples){
			this.values.add("");
		}
		if(stsmf!=null){
			try{
				BufferedReader br = new BufferedReader(new FileReader(stsmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] fields=line.split("\t", 1000);
					String sample=fields[3];
					if(samples.contains(sample)){
						this.values.set(this.samples.indexOf(sample), fields[2]);
					}
					else{
						br.close();
						return;
					}
				}
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}		
		}
	}
	public Vector<String> getValues(){
		return this.values;
	}
	public Vector<String> getSamples(){
		return this.samples;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
