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
package fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression;

import java.io.File;
import java.util.Vector;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.RemoveRawFileListener;
import fr.sanofi.fcl4transmart.controllers.listeners.geneExpression.SelectGeneRawFileListener;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class allows the creation of the composite to select gene expression data raw file
 */
public class SelectRawFileUI implements WorkItf{
	private DataTypeItf dataType;
	private Text pathField;
	private String path;
	private ListViewer viewer;
	public SelectRawFileUI(DataTypeItf dataType){
		this.dataType=dataType;
		this.path="";
	}
	@Override
	public Composite createUI(Composite parent){
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
		
		Composite pathPart=new Composite(scrolledComposite, SWT.NONE);gd=new GridLayout();
		gd.numColumns=3;
		gd.horizontalSpacing=5;
		gd.verticalSpacing=5;
		pathPart.setLayout(gd);
		
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
		
		Button add=new Button(scrolledComposite, SWT.PUSH);
		add.setText("Add files");
		add.addListener(SWT.Selection, new SelectGeneRawFileListener(this, this.dataType));
		
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
		this.viewer.setInput(((GeneExpressionData)this.dataType).getRawFiles());
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
	public void updateViewer(){
		this.viewer.setInput(((GeneExpressionData)this.dataType).getRawFiles());
		this.displayNames();
	}
	public Vector<File> getSelectedRemovedFile(){
		Vector<File> files=new Vector<File>();
		String[] paths=this.viewer.getList().getSelection();
		for(int i=0; i<paths.length; i++){
			if(((GeneExpressionData)this.dataType).getRawFilesNames().contains(paths[i])){
				files.add(new File(((GeneExpressionData)this.dataType).getPath()+File.separator+paths[i]));
			}
		}
		return files; 
}
	public void displayNames(){
		for(int i=0; i<this.viewer.getList().getItemCount(); i++){
			this.viewer.getList().setItem(i, ((File)this.viewer.getElementAt(i)).getName());
		}
	}
	public String getPath(){
		return this.pathField.getText();
	}
	public boolean confirm(String message){
		return MessageDialog.openConfirm(new Shell(), "Confirm", message);
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
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
