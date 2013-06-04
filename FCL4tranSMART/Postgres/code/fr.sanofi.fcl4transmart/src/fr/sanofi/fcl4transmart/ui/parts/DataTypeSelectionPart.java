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

import java.util.Vector;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.core.services.events.IEventBroker;
import org.eclipse.e4.ui.workbench.modeling.ESelectionService;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ListViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import fr.sanofi.fcl4transmart.controllers.DataTypeSelectionController;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;

import org.eclipse.e4.ui.di.UIEventTopic;

/**
 *This class handles the data type selection part
 */
public class DataTypeSelectionPart {
	private ListViewer viewer;
	private DataTypeSelectionController dataTypeSelectionController;
	private Composite parent;
	@Inject private ESelectionService selectionService;
	@Inject  private IEventBroker eventBroker;
	@PostConstruct
	public void createControls(Composite parent) {
		this.parent=parent;
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		parent.setLayout(gd);
		
		this.viewer=new ListViewer(this.parent, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
		this.viewer.getControl().setLayoutData(new GridData(GridData.FILL_BOTH));

		viewer.setContentProvider(new IStructuredContentProvider(){
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
		viewer.setInput(new Vector<DataTypeItf>());
		/*viewer.addSelectionChangedListener(new ISelectionChangedListener() {
			@Override
			public void selectionChanged(SelectionChangedEvent event) {
				IStructuredSelection selection = (IStructuredSelection) viewer.getSelection();
				selectionService.setSelection(selection.getFirstElement());
			}
		});*/
		this.viewer.getList().addSelectionListener(new SelectionListener(){
			@Override
			public void widgetSelected(SelectionEvent e) {
				eventBroker.send("dataTypeChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
				
			}

			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
		});
		this.dataTypeSelectionController=new DataTypeSelectionController(this);
	}
	public void setList(Vector<DataTypeItf> dataTypes){
		 viewer.setInput(dataTypes);
		 parent.layout(true, true);
	}
	/**
	 *Update data type if an event indicates that the selected study changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("studyChanged/*") StudyItf selectedStudy) {  
		if (selectedStudy != null) {
			  this.dataTypeSelectionController.selectionChanged(selectedStudy);
			  this.viewer.getList().select(0);
			  eventBroker.send("dataTypeChanged/syncEvent",viewer.getElementAt(viewer.getList().getSelectionIndex()));
		  }
	} 
	/**
	*Update data types if an event indicates that a new study has been addes
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
