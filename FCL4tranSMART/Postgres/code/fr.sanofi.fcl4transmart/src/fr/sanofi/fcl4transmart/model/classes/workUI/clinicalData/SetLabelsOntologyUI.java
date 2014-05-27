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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.BioportalController;
import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SetLabelsOntologyListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class allows the creation of the composite to set properties labels and codes
 */
public class SetLabelsOntologyUI implements WorkItf{
	private DataTypeItf dataType;
	private Vector<String> newLabels;
	private Vector<Text> newLabelsFields;
	private Vector<String> headers;
	private Vector<Text> headerFields;
	private Vector<String> codes;
	private Vector<Text> codesFields;
	private Vector<Button> buttons;
	private int i;
	public SetLabelsOntologyUI(DataTypeItf dataType){
		this.dataType=dataType;
	}
	@Override
	public Composite createUI(Composite parent){

		this.newLabelsFields=new Vector<Text>();
		this.codesFields=new Vector<Text>();
		this.headerFields=new Vector<Text>();
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
		
		if(((ClinicalData)this.dataType).getCMF()==null){
			this.displayMessage("Error: no column mapping file");
			scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
			return composite;
		}
		
		this.initiate();
		
		Composite part=new Composite(scrolledComposite, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=4;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		gd.verticalSpacing=5;
		gd.horizontalSpacing=5;
		part.setLayout(gd);
		
		Label c1=new Label(part, SWT.NONE);
		c1.setText("Column");

		Label c2=new Label(part, SWT.NONE);
		c2.setText("New label");
		Label c3=new Label(part, SWT.NONE);
		c3.setText("Controled vocabulary code");
		Label c4=new Label(part, SWT.NONE);
		c4.setText("");
		
		buttons=new Vector<Button>();
		for(this.i=0; this.i<this.headers.size(); this.i++){	
			/*Label title=new Label(part, SWT.NONE);
			title.setText(this.headers.elementAt(this.i));
			title.setLayoutData(new GridData(GridData.FILL_BOTH));*/
			
			Text title=new Text(part, SWT.BORDER);
			title.setText(this.headers.elementAt(this.i));
			title.setEditable(false);
			this.headerFields.add(title);
			GridData gridData = new GridData();
			gridData.widthHint=150;
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			title.setLayoutData(gridData);
			
			Text field=new Text(part, SWT.BORDER);
			field.setText(this.newLabels.elementAt(this.i));
			this.newLabelsFields.add(field);
			field.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=newLabelsFields.indexOf(e.getSource());
					newLabels.setElementAt(newLabelsFields.elementAt(n).getText(), n);
				}
			});
			gridData = new GridData();
			gridData.widthHint=150;
			gridData.horizontalAlignment = SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			field.setLayoutData(gridData);
			
			Text field2=new Text(part, SWT.BORDER);
			field2.setText(this.codes.elementAt(this.i));
			this.codesFields.add(field2);
			field2.addModifyListener(new ModifyListener(){
				public void modifyText(ModifyEvent e){
					int n=codesFields.indexOf(e.getSource());
					codes.setElementAt(codesFields.elementAt(n).getText(), n);
				}
			});
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.widthHint=150;
			gridData.grabExcessHorizontalSpace = true;
			field2.setLayoutData(gridData);
			
			Button ont=new Button(part, SWT.PUSH);
			ont.setText("Bioportal");
			gridData = new GridData();
			gridData.horizontalAlignment = SWT.FILL;
			gridData.widthHint=75;
			gridData.grabExcessHorizontalSpace = true;
			ont.setLayoutData(gridData);
			ont.addSelectionListener(new SelectionListener(){
				@Override
				public void widgetSelected(SelectionEvent e) {
					if(!BioportalController.testBioportalConnection()){
						displayMessage("Connection to bioportal is not possible");
					}
					else{
						String[] terms=BioportalController.getTerms();
						if(terms!=null && terms[0]!=null && terms[1]!=null){
							int n=buttons.indexOf((Button)e.getSource());
							newLabelsFields.get(n).setText(terms[0]);
							codesFields.get(n).setText(terms[1]);
						}
					}
				}

				@Override
				public void widgetDefaultSelected(SelectionEvent e) {
					// TODO Auto-generated method stub
					
				}
			});
			buttons.add(ont);
		}
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SetLabelsOntologyListener(this, this.dataType));
		
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public void displayMessage(String message){
		int style = SWT.ICON_WARNING | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	private void initiate(){
		this.newLabels=new Vector<String>();
		this.headers=new Vector<String>();
		this.codes=new Vector<String>();
		for(File raw: ((ClinicalData)this.dataType).getRawFiles()){
			for(String s: FileHandler.getHeadersFromCmf(((ClinicalData)this.dataType).getCMF(), raw)){
				String l=FileHandler.getDataLabel(((ClinicalData)this.dataType).getCMF(), raw, s);
				if(l.compareTo("DATA_LABEL")!=0){
					this.headers.add(raw.getName()+" - "+s);
					if(l.compareTo(s)!=0){
						this.newLabels.add(l);
					}
					else{
						this.newLabels.add("");
					}
					//
					this.codes.add(FileHandler.getCodeFromHeader(((ClinicalData)this.dataType).getCMF(), raw, s));
				}
			}
		}
	}
	public Vector<String> getHeaders(){
		return this.headers;
	}
	public Vector<String> getNewLabels(){
		return this.newLabels;
	}
	public Vector<String> getCodes(){
		return this.codes;
	}
	@Override
	public boolean canCopy() {
		return true;
	}
	@Override
	public boolean canPaste() {
		return true;
	}
	@Override
	public Vector<Vector<String>> copy() {
		Vector<Vector<String>> data=new Vector<Vector<String>>();
		data.add(headers);
		data.add(newLabels);
		data.add(codes);
		return data;
	}
	@Override
	public void paste(Vector<Vector<String>> data) {
		if(data.size()>0){
			int l=this.newLabels.size();
			if(data.get(0).size()<l) l=data.get(0).size();
			for(int i=0; i<l; i++){
				this.newLabels.set(i, data.get(0).get(i));
				this.newLabelsFields.get(i).setText(data.get(0).get(i));
			}
		}
		if(data.size()>1){
			int l=this.codes.size();
			if(data.get(1).size()<l) l=data.get(1).size();
			for(int i=0; i<l; i++){
				this.codes.set(i, data.get(1).get(i));
				this.codesFields.get(i).setText(data.get(1).get(i));
			}
		}
	}
	@Override
	public void mapFromClipboard(Vector<Vector<String>> data) {
		if(data.size()<2) return;
		for(int i=0; i<data.get(0).size(); i++){
			int index=headers.indexOf(data.get(0).get(i));
			if(index!=-1){
				if(data.get(1).size()>i){
					newLabels.set(index, data.get(1).get(i));
					this.newLabelsFields.get(index).setText(newLabels.get(index));
				}
				if(data.size()>2 && data.get(2).size()>i){
					codes.set(index, data.get(2).get(i));
					this.codesFields.get(index).setText(codes.get(index));
				}
			}
		}
	}
}
