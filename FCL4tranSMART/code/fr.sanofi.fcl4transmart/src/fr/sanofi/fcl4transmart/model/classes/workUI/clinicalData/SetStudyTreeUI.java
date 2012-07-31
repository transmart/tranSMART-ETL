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

import org.eclipse.jface.viewers.AbstractTreeViewer;
import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.controllers.StudyTreeController;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SetStudyTreeListener;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.StudyContentProvider;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.StudyTree;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.TreeNode;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SetStudyTreeUI implements WorkItf{
	private DataTypeItf dataType;
	private TreeViewer viewer;
	private StudyTree studyTree;
	private Text newChildField;
	private Combo newLabelField;
	private TreeNode root;
	public SetStudyTreeUI(DataTypeItf dataType){
		this.dataType=dataType;
		this.root=new TreeNode(this.dataType.getStudy().toString(), null, false);
		this.studyTree=new StudyTree(root);
	}
	@Override
	public Composite createUI(Composite parent){
		this.root=new TreeNode(this.dataType.getStudy().toString(), null, false);
		this.studyTree=new StudyTree(root);
		if(((ClinicalData)this.dataType).getCMF()!=null){
			this.root=new StudyTreeController(this.root, this.dataType).buildTree(((ClinicalData)this.dataType).getCMF());
		}
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
		body.setLayoutData(new GridData(GridData.FILL_BOTH));
		gd=new GridLayout();
		gd.numColumns=2;
		body.setLayout(gd);

		
		viewer = new TreeViewer(body, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
		viewer.setContentProvider(new StudyContentProvider());
		viewer.setAutoExpandLevel(AbstractTreeViewer.ALL_LEVELS);

		viewer.setInput(this.studyTree);
		GridData gridData = new GridData(GridData.FILL_BOTH);
		gridData.horizontalAlignment = SWT.FILL;
		gridData.verticalAlignment=SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace=true;
		gridData.heightHint=300;
		gridData.widthHint=200;
		this.viewer.getControl().setLayoutData(gridData);
		viewer.setLabelProvider(new ColumnLabelProvider() {
		    @Override
		    public String getText(Object element) {
		        return element.toString();
		    }

		    @Override
		    public Color getBackground(Object element) {
		    	if(((TreeNode)element).isLabel()){
		    		return new Color(Display.getCurrent(), 212, 212, 212);
		    	}
		    	return null;
		    }
		});

		
		Composite leftPart=new Composite(body, SWT.NONE);
		leftPart.setLayout(new RowLayout(SWT.VERTICAL));
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.verticalAlignment=SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace=true;
		leftPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite newChildPart=new Composite(leftPart, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		newChildPart.setLayout(gd);
		Label newChildLabel=new Label(newChildPart, SWT.NONE);
		newChildLabel.setText("New node: ");
		this.newChildField=new Text(newChildPart, SWT.BORDER);
		
		Button addChild=new Button(leftPart, SWT.PUSH);
		addChild.setText("Add node");
		
		addChild.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				IStructuredSelection selection=(IStructuredSelection)viewer.getSelection();
				TreeNode node;
				if(selection.iterator().hasNext()){
					node=(TreeNode)selection.iterator().next();
				}
				else{
					displayMessage("Select a node first");
					return;
				}
				if(node.isLabel()){
					displayMessage("It is not possible to add a node to a label");
					return;
				}
				if(newChildField.getText().compareTo("")==0){
					displayMessage("Node name is empty");
					return;
				}
				if(node.getChild(newChildField.getText())!=null){
					displayMessage("This node already exists");
				}
				node.addChild(new TreeNode(newChildField.getText(), node, false));
				viewer.setExpandedState(node, true);
				viewer.refresh();
			}
		});
		
		Button remove=new Button(leftPart,SWT.PUSH);
		remove.setText("Remove a node");
		remove.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				IStructuredSelection selection=(IStructuredSelection)viewer.getSelection();
				TreeNode node;
				if(selection.iterator().hasNext()){
					node=(TreeNode)selection.iterator().next();
				}
				else{
					displayMessage("Select a node first");
					return;
				}
				node.getParent().removeChild(node);
				viewer.refresh();
			}
		});
		
		Composite newLabelPart=new Composite(leftPart, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		newLabelPart.setLayout(gd);
		Label newLabelLabel=new Label(newLabelPart, SWT.NONE);
		newLabelLabel.setText("Choose label");
		this.newLabelField=new Combo(newLabelPart, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
	    this.newLabelField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		this.newLabelField.setLayoutData(gridData);
		for(File file: ((ClinicalData)this.dataType).getRawFiles()){
			for(String s: FileHandler.getHeaders(file)){
		    	this.newLabelField.add(file.getName()+" - "+s);
		    }
		}
		
		Button addLabel=new Button(leftPart, SWT.PUSH);
		addLabel.setText("Add label");
		addLabel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				// TODO Auto-generated method stub
				IStructuredSelection selection=(IStructuredSelection)viewer.getSelection();
				TreeNode node;
				if(selection.iterator().hasNext()){
					node=(TreeNode)selection.iterator().next();
				}
				else{
					displayMessage("Select a node first");
					return;
				}
				if(newLabelField.getText().compareTo("")==0){
					displayMessage("Choose a label");
					return;
				}
				if(node.getParent()!=null && node.getParent().getParent()!=null && node.getParent().isLabel()){
					displayMessage("This node parent is already a label");
					return;
				}
				if(node.getChild(newLabelField.getText())!=null){
					displayMessage("This label already exists");
					return;
				}
				node.addChild(new TreeNode(newLabelField.getText(), node, true));
				viewer.setExpandedState(node, true);
				viewer.refresh();
			}
		});
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new SetStudyTreeListener(this, this.dataType));
		

		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		
		return composite;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public TreeNode getRoot(){
		return this.root;
	}
}
