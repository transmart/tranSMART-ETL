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
package fr.sanofi.fcl4transmart.ui.parts;

import java.io.File;
import java.util.Vector;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.core.services.events.IEventBroker;
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;

import fr.sanofi.fcl4transmart.controllers.UsedFilesController;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
/**
 *This class handles the file viewer list part
 */
public class UsedFilesPart {
	private ListViewer viewer;
	private UsedFilesController usedFilesController;
	private Composite parent;
	@Inject private IEventBroker eventBroker;
	private static IEventBroker staticEventBroker;
	@PostConstruct
	public void createControls(Composite parent) {
		UsedFilesPart.staticEventBroker=this.eventBroker;
		this.parent=parent;
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		parent.setLayout(gd);
		
		this.viewer=new ListViewer(this.parent, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
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
		this.viewer.setInput(new Vector<File>());
		this.viewer.getList().addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				eventBroker.send("fileChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
			}
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
		});
		this.usedFilesController=new UsedFilesController(this);
	}
	public void setList(Vector<File> files){
		 this.viewer.setInput(files);
		 this.displayNames();
		 this.parent.layout(true, true);
	}
	/**
	 *Updates the file list if an event indicates that data type changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("dataTypeChanged/*") DataTypeItf selectedDataType) {
		if (selectedDataType != null) {
			  this.usedFilesController.selectionChanged(selectedDataType);
			  this.viewer.getList().select(0);
			  eventBroker.send("fileChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
		  }
	} 
	/**
	 *Updates the file list if an event indicates that files have been modified
	 */
	@Inject
	void fileEventReceived(@Optional @UIEventTopic("filesChanged/*") DataTypeItf selectedDataType) {
		if (selectedDataType != null) {
			  this.usedFilesController.selectionChanged(selectedDataType);
			  this.viewer.getList().select(0);
		  }
	}
	/**
	 *Allows to display file names in the list instead of file paths
	 */
	public void displayNames(){
		for(int i=0; i<this.viewer.getList().getItemCount(); i++){
			this.viewer.getList().setItem(i, ((File)this.viewer.getElementAt(i)).getName());
		}
	}	
	/**
	 *Send an event indicating that the selected file changed
	 */
	public static void sendFilesChanged(DataTypeItf dataType){
		if(UsedFilesPart.staticEventBroker!=null){
			UsedFilesPart.staticEventBroker.send("filesChanged/syncEvent",dataType);
		}
	}
	/**
	 *Sets an empty list if an event indicates that a new study has been created (no files)
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("newStudy/*") String string) {
		if(string!=null){
			if(string.compareTo("new workspace")==0){
				this.viewer.setInput(new Vector<String>());
			}
		}
	} 
}