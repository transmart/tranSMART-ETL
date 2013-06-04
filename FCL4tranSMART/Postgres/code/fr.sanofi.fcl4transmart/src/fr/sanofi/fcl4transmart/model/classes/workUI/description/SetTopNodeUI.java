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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.TopNodeController;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.StudyContentProvider;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.StudyTree;
import fr.sanofi.fcl4transmart.model.classes.TreeNode;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class allows the creation of the composite to set the study top node
 */
public class SetTopNodeUI implements WorkItf{
	private DataTypeItf dataType;
	private TreeViewer viewer;
	private StudyTree studyTree;
	private Text titleField;
	private Text newTextField;
	private TreeNode root;
	private TopNodeController controller;
	private boolean testDemodata;
	private boolean testMetadata;
	private boolean isSearching;
	public SetTopNodeUI(DataTypeItf dataType){
		this.dataType=dataType;
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
				testDemodata=RetrieveData.testDemodataConnection();
				testMetadata=RetrieveData.testMetadataConnection();
				root=new TreeNode("Dataset explorer", null, false);
				studyTree=new StudyTree(root);
				controller=new TopNodeController(root, dataType, studyTree);
				root=controller.buildTree();
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
		gridData.widthHint=300;
		this.viewer.getControl().setLayoutData(gridData);
		viewer.setLabelProvider(new ColumnLabelProvider() {
		    @Override
		    public String getText(Object element) {
		        return element.toString();
		    }

		    @Override
		    public Color getBackground(Object element) {
		    	if(((TreeNode)element).isStudyRoot()){
		    		return new Color(Display.getCurrent(), 237, 91, 67);
		    	}
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
		newChildLabel.setText("Free text: ");
		this.newTextField=new Text(newChildPart, SWT.BORDER);
		gridData = new GridData();
		gridData.widthHint=100;
		this.newTextField.setLayoutData(gridData);
		
		Button addChild=new Button(leftPart, SWT.PUSH);
		addChild.setText("Add free text");
		
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
					displayMessage("It is not possible to add a node to a study");
					return;
				}
				if(newTextField.getText().compareTo("")==0){
					displayMessage("Node name is empty");
					return;
				}
				if(node.getChild(newTextField.getText())!=null){
					displayMessage("This node already exists");
				}
				node.addChild(new TreeNode(newTextField.getText(), node, false));
				viewer.setExpandedState(node, true);
				viewer.refresh();
			}
		});
				
		Composite newLabelPart=new Composite(leftPart, SWT.NONE);
		gd=new GridLayout();
		gd.numColumns=2;
		newLabelPart.setLayout(gd);
		Label newLabelLabel=new Label(newLabelPart, SWT.NONE);
		newLabelLabel.setText("Study title");
		this.titleField=new Text(newLabelPart, SWT.BORDER);
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=100;
		this.titleField.setLayoutData(gridData);
		
		Button addLabel=new Button(leftPart, SWT.PUSH);
		addLabel.setText("Add the study");
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
				if(titleField.getText().compareTo("")==0){
					displayMessage("Choose a title");
					return;
				}
				if(node.isLabel()){
					displayMessage("This node is already a study");
					return;
				}
				if(controller.getTopNode(root).compareTo("")!=0){
					displayMessage("The study has already been added");
					return;
				}
				TreeNode newNode=new TreeNode(titleField.getText(), node, true);
				newNode.setIsStudyTree(true);
				studyTree.setHasStudy(true);
				node.addChild(newNode);
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
				if(node.isLabel() && !node.isStudyRoot()){
					displayMessage("You can not remove another study");
					return;
				}
				if(!checkRemoveNode(node)){
					displayMessage("This node contains a study");
					return;
				}
				if(node.getParent()==null){
					displayMessage("You can not remove the root of the studies");
					return;
				}
				node.getParent().removeChild(node);
				viewer.refresh();
			}
		});
		
		Button ok=new Button(scrolledComposite, SWT.PUSH);
		ok.setText("OK");
		ok.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				controller.writeTopNode();
				displayMessage("Top node has been saved");
			}
		});
		
		if(this.testDemodata && this.testMetadata){
			Label dbLabel=new Label(scrolledComposite, SWT.NONE);
			dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
		}else{
			Label warn=new Label(scrolledComposite, SWT.NONE);
			warn.setText("Warning: connection to database is not possible");
			ok.setEnabled(false);
		}

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
	public boolean checkRemoveNode(TreeNode node){
		boolean remove=true;
		for(TreeNode child: node.getChildren()){
			if(!checkRemoveNode(child)){
				remove=false;
			}
		}
		if(node.isLabel() && !node.isStudyRoot()){
			remove=false;
		}
		return remove;
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
