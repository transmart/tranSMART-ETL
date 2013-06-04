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
package fr.sanofi.fcl4transmart.model.classes.workUI.description;

import java.util.Vector;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.listeners.description.ChangeNameListener;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class allows the creation of the composite to change the study name
 */
public class ChangeNameUI implements WorkItf{
	private StudyItf study;
	private Text nameField;
	private String name;
	private boolean isLoaded;
	private boolean testMetadata;
	private boolean isSearching;
	public ChangeNameUI(StudyItf study){
		this.study=study;
		this.name=this.study.toString();
	}
	@Override
	public Composite createUI(Composite parent){
		
		Shell shell=new Shell();
		shell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(shell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(shell, SWT.NONE);
		searching.setText("Searching...");
		shell.open();
		this.isSearching=true;
		new Thread(){
			public void run() {
				isLoaded=RetrieveData.isLoaded(study.toString().toUpperCase());
				testMetadata=RetrieveData.testMetadataConnection();
				isSearching=false;
			}
        }.start();
        Display display=WorkPart.display();
        while(this.isSearching){
        	if (!display.readAndDispatch()) {
                display.sleep();
              }	
        }
		shell.close();
		
		Composite composite=new Composite(parent, SWT.NONE);
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		composite.setLayout(gd);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		composite.setLayoutData(gridData);
		
		ScrolledComposite scroller=new ScrolledComposite(composite, SWT.H_SCROLL | SWT.V_SCROLL);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		scroller.setLayoutData(gridData);
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		scrolledComposite.setLayoutData(gridData);
		
		Composite namePart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 2;
		namePart.setLayout(layout);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		namePart.setLayoutData(gridData);
		Label nameLabel=new Label(namePart, SWT.NONE);
		nameLabel.setText("Identifier: ");
		gridData = new GridData();
		gridData.widthHint=60;
		nameLabel.setLayoutData(gridData);
		this.nameField=new Text(namePart, SWT.BORDER);
		this.nameField.setText(this.name);
		if(this.isLoaded) this.nameField.setEditable(false);
		this.nameField.addModifyListener(new ModifyListener(){
			@Override
			public void modifyText(ModifyEvent e) {
				// TODO Auto-generated method stub
				name=nameField.getText();
			}
		});
		gridData = new GridData();
		gridData.widthHint=100;
		this.nameField.setLayoutData(gridData);
		
		
		Button change=new Button(scrolledComposite, SWT.PUSH);
		change.setText("OK");
		gridData = new GridData();
		gridData.widthHint=15;
		change.addListener(SWT.Selection, new ChangeNameListener(this, this.study));
		
		if(this.testMetadata){
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
