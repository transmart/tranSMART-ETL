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
import org.eclipse.jface.dialogs.MessageDialog;
import java.util.Vector;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.RemoveRawFileListener;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.SelectClinicalRawFileListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class allows the creation of the composite to select clinical raw data files
 */
public class SelectRawFilesUI implements WorkItf{
	private DataTypeItf dataType;
	private Text pathField;
	private String path;
	private Combo fileTypeField;
	private ListViewer viewer;
	private boolean isLoading;
	private Display display;
	private Shell loadingShell;
	private String format;
	private String message="";
	public SelectRawFilesUI(DataTypeItf dataType){
		this.dataType=dataType;
		this.path="";
		this.format="";
	}
	@Override
	public Composite createUI(Composite parent){

   		this.display=WorkPart.display();
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
		scroller.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite scrolledComposite=new Composite(scroller, SWT.NONE);
		scroller.setContent(scrolledComposite); 
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		scrolledComposite.setLayout(layout);
		scrolledComposite.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite pathPart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 3;
		pathPart.setLayout(layout);
		pathPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Label pathLabel=new Label(pathPart, SWT.NONE);
		pathLabel.setText("Path: ");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		pathLabel.setLayoutData(gridData);
		this.pathField=new Text(pathPart, SWT.BORDER);
		this.pathField.addModifyListener(new ModifyListener(){
			@Override
			public void modifyText(ModifyEvent e) {
				// TODO Auto-generated method stub
				path=pathField.getText();
			}
		});
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.widthHint=150;
		gridData.grabExcessHorizontalSpace = true;
		this.pathField.setLayoutData(gridData);
		Button browse=new Button(pathPart, SWT.PUSH);
		browse.setText("Browse");
		browse.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				FileDialog fd=new FileDialog(new Shell(), SWT.MULTI);
				fd.open();
				String[] filenames=fd.getFileNames();
				String filterPath=fd.getFilterPath(); 
				path="";
				for(int i=0; i<filenames.length; i++){
					if(path.compareTo("")==0){
						if(filterPath!=null && filterPath.trim().length()>0){
							path+=filterPath+File.separator+filenames[i];
						}
						else{
							path+=filenames[i];
						}
					}
					else{
						if(filterPath!=null && filterPath.trim().length()>0){
							path+=File.pathSeparator+filterPath+File.separator+filenames[i];
						}
						else{
							path+=File.pathSeparator+filenames[i];
						}
					}
				}
				pathField.setText(path);
			}		
		});
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		browse.setLayoutData(gridData);
		
		Composite fileTypePart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 2;
		fileTypePart.setLayout(layout);
		Label fileTypeLabel=new Label(fileTypePart, SWT.NONE);
		fileTypeLabel.setText("Format: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		fileTypeLabel.setLayoutData(gridData);
		this.fileTypeField=new Combo(fileTypePart, SWT.DROP_DOWN | SWT.BORDER );
	    this.fileTypeField.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
		this.fileTypeField.add("Tab delimited raw file");
		this.fileTypeField.add("SOFT");
		this.fileTypeField.addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				if(fileTypeField.getSelectionIndex()==-1){
					format="";
				}
				else{
					format=fileTypeField.getItem(fileTypeField.getSelectionIndex());
				}
			}
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
		});
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=120;
		this.fileTypeField.setLayoutData(gridData);
		
		Button add=new Button(scrolledComposite, SWT.PUSH);
		add.setText("Add files");
		add.addListener(SWT.Selection, new SelectClinicalRawFileListener(this, this.dataType));
		
		Label filesLabel=new Label(scrolledComposite, SWT.NONE);
		filesLabel.setText("\nRaw data files:");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		filesLabel.setLayoutData(gridData);
		
		Composite filesPart=new Composite(scrolledComposite, SWT.NONE);
		layout = new GridLayout();
		layout.numColumns = 2;
		filesPart.setLayout(layout);
		
		this.viewer=new ListViewer(filesPart);
		this.viewer.getControl().setLayoutData(new GridData(GridData.FILL_BOTH));

		this.viewer.setContentProvider(new IStructuredContentProvider(){
			public Object[] getElements(Object inputElement) {
				@SuppressWarnings("rawtypes")
				Vector v = (Vector)inputElement;
				return v.toArray();
			}
			public void dispose() {
				// TODO Auto-generated method stub

			}
			public void inputChanged(Viewer viewer, Object oldInput,
					Object newInput) {
				// TODO Auto-generated method stub
				
			}
		});	
		this.viewer.setInput(((ClinicalData)this.dataType).getRawFiles());
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=100;
		gridData.heightHint=125;
		this.viewer.getControl().setLayoutData(gridData);
		this.displayNames();
		
		Button remove=new Button(filesPart, SWT.PUSH);
		remove.setText("Remove selected files");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		remove.setLayoutData(gridData);
		remove.addListener(SWT.Selection, new RemoveRawFileListener(this.dataType, this));

		scrolledComposite.setSize(scrolledComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		return composite;
	}
	public String getPath(){
		return this.path;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
	public boolean confirm(String message){
		return MessageDialog.openConfirm(new Shell(), "Confirm", message);
	}
	public String getFormat(){
		return this.format;
	}
	public void displayNames(){
		for(int i=0; i<this.viewer.getList().getItemCount(); i++){
			this.viewer.getList().setItem(i, ((File)this.viewer.getElementAt(i)).getName());
		}
	}
	public void updateViewer(){
		this.viewer.setInput(((ClinicalData)this.dataType).getRawFiles());
		this.displayNames();
	}
	public Vector<File> getSelectedRemovedFile(){
			Vector<File> files=new Vector<File>();
			String[] paths=this.viewer.getList().getSelection();
			for(int i=0; i<paths.length; i++){
				if(((ClinicalData)this.dataType).getRawFilesNames().contains(paths[i])){
					files.add(new File(((ClinicalData)this.dataType).getPath()+File.separator+paths[i]));
				}
			}
			return files; 
	}
	public void openLoadingShell(){
		this.isLoading=true;
		this.message="";
		this.loadingShell=new Shell(this.display);
		this.loadingShell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.loadingShell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(this.loadingShell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(this.loadingShell, SWT.NONE);
		searching.setText("Creating new file...");
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
	public void setMessage(String message){
		this.message=message;
	}
	public void setIsLoading(boolean bool){
		this.isLoading=bool;
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
