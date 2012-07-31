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
package fr.sanofi.fcl4transmart.model.classes.workUI.description;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.description.ChangeNameListener;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class ChangeNameUI implements WorkItf{
	private StudyItf study;
	private Text nameField;
	private String name;
	public ChangeNameUI(StudyItf study){
		this.study=study;
		this.name=this.study.toString();
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
		
		Composite namePart=new Composite(scrolledComposite, SWT.NONE);
		namePart.setLayout(new RowLayout(SWT.HORIZONTAL));
		Label nameLabel=new Label(namePart, SWT.NONE);
		nameLabel.setText("Name: ");
		this.nameField=new Text(namePart, SWT.BORDER);
		this.nameField.setText(this.name);
		if(RetrieveData.isLoaded(this.study.toString().toUpperCase())) this.nameField.setEditable(false);
		this.nameField.addModifyListener(new ModifyListener(){
			@Override
			public void modifyText(ModifyEvent e) {
				// TODO Auto-generated method stub
				name=nameField.getText();
			}
		});
		
		Button change=new Button(scrolledComposite, SWT.PUSH);
		change.setText("OK");
		change.addListener(SWT.Selection, new ChangeNameListener(this, this.study));
		
		if(RetrieveData.testMetadataConnection()){
			Label dbLabel=new Label(scrolledComposite, SWT.NONE);
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
		}else{
			Label warn=new Label(scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
		}

		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public String getName(){
		return this.nameField.getText();
	}
	public void displayMessage(String message){
		int style = SWT.ICON_WARNING | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}
