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

import org.eclipse.jface.viewers.AbstractTreeViewer;
import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Color;
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
import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.TopNodeController;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.StudyContentProvider;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.LoadGeneExpressionDataListener;
import fr.sanofi.fcl4transmart.model.classes.StudyTree;
import fr.sanofi.fcl4transmart.model.classes.TreeNode;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class LoadDataUI implements WorkItf{
	private DataTypeItf dataType;
	private TreeNode root;
	private StudyTree studyTree;
	private TreeViewer viewer;
	private TopNodeController topNodeController;
	private String topNode;
	private Display display;
	private boolean isSearching;
	private boolean testTm_cz;
	private boolean testTm_lz;
	private boolean testDeapp;
	private String message;
	private Shell loadingShell;
	private boolean isLoading;
	public LoadDataUI(DataTypeItf dataType){
		this.dataType=dataType;
		this.topNode="";
	}
	@Override
	public Composite createUI(Composite parent){
		this.display=WorkPart.display();
		Shell shell=new Shell(this.display);
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
				testTm_cz=RetrieveData.testTm_czConnection();
				testTm_lz=RetrieveData.testTm_lzConnection();
				testDeapp=RetrieveData.testDeappConnection();
				root=new TreeNode("Dataset explorer", null, false);
				studyTree=new StudyTree(root);	
				topNodeController=new TopNodeController(root, dataType, studyTree);
				root=topNodeController.buildTree();	
				topNode=dataType.getStudy().getTopNode();
				isSearching=false;
			}
        }.start();
        this.display=WorkPart.display();
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
		composite.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		ScrolledComposite scroller=new ScrolledComposite(composite, SWT.H_SCROLL | SWT.V_SCROLL);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		scroller.setLayout(gd);
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		scrolledComposite.setLayoutData(new GridData(GridData.FILL_BOTH));

		this.topNode=this.dataType.getStudy().getTopNode();
		if(topNode!=null && topNode.compareTo("")!=0){
			viewer = new TreeViewer(scrolledComposite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
			viewer.setContentProvider(new StudyContentProvider());
			viewer.setAutoExpandLevel(AbstractTreeViewer.ALL_LEVELS);

			viewer.setInput(this.studyTree);
			GridData gridData = new GridData(GridData.FILL_BOTH);
			gridData.horizontalAlignment = SWT.FILL;
			gridData.verticalAlignment=SWT.FILL;
			gridData.grabExcessHorizontalSpace = true;
			gridData.grabExcessVerticalSpace=true;
			gridData.heightHint=300;
			gridData.widthHint=250;
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
		}
		else{
			Label label=new Label(scrolledComposite, SWT.NONE);
			label.setText("Please first choose a study node in the description data type");
		}
		
		Button button=new Button(scrolledComposite, SWT.PUSH);
		button.setText("Load");
		
		if(topNode!=null && topNode.compareTo("")!=0){
			if(this.testTm_cz && this.testTm_lz && this.testDeapp){
				button.addListener(SWT.Selection, new LoadGeneExpressionDataListener(this, this.dataType));
				Label dbLabel=new Label(scrolledComposite, SWT.NONE);
				dbLabel.setText("You are connected to database '"+PreferencesHandler.getDb()+"'");
			}
			else{
				button.setEnabled(false);
				Label warn=new Label(scrolledComposite, SWT.NONE);
				warn.setText("Warning: connection to database is not possible");
			}
		}
		else{
			button.setEnabled(false);
		}
		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public String getTopNode(){
		return this.topNode;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public void openLoadingShell(){
		this.message="";
		this.isLoading=true;
		this.loadingShell=new Shell(this.display);
		this.loadingShell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.loadingShell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(this.loadingShell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(this.loadingShell, SWT.NONE);
		searching.setText("Loading...");
		this.loadingShell.open();
	}
	public void waitForThread(){
        while(this.isLoading){
        	if (!this.display.readAndDispatch()) {
                this.display.sleep();
              }	
        }
        this.loadingShell.close();	
        if(this.message.compareTo("")!=0){
        	this.displayMessage(message);
        }
	}
	public void setIsLoading(boolean bool){
		this.isLoading=bool;
	}
	public void setMessage(String message){
		this.message=message;
	}
}
