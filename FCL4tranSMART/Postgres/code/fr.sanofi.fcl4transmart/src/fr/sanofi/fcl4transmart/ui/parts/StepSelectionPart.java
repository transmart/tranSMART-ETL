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
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import fr.sanofi.fcl4transmart.controllers.StepSelectionController;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import org.eclipse.jface.viewers.TableViewer;

/**
 *This class handles the step selection part
 */
public class StepSelectionPart {
	private TableViewer viewer;
	private StepSelectionController stepSelectionController;
	private Composite parent;
	private int lastSelectionIndex;
	@Inject  private IEventBroker eventBroker;
	@PostConstruct
	public void createControls(Composite parent) {
		this.lastSelectionIndex=-1;
		this.parent=parent;
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		this.parent.setLayout(gd);
		
		this.viewer=new TableViewer(this.parent, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER);
		this.viewer.getControl().setLayoutData(new GridData(GridData.FILL_BOTH));

		viewer.setContentProvider(new ArrayContentProvider());	
		viewer.setInput(new Vector<StepItf>());
		viewer.setLabelProvider(new ColumnLabelProvider() {
		    @Override
		    public String getText(Object element) {
		        return element.toString();
		    }

		    @Override
		    public Color getForeground(Object element) {
		    	if(!((StepItf)element).isAvailable()){
		    		return new Color(Display.getCurrent(), 212, 212, 212);
		    	}
		    	return null;
		    }
		});
		
		this.viewer.getTable().addSelectionListener(new SelectionListener(){

			@Override
			public void widgetSelected(SelectionEvent e) {
				@SuppressWarnings("unchecked")
				StepItf step=((Vector<StepItf>)viewer.getInput()).get(viewer.getTable().getSelectionIndex());
				if(step.isAvailable()){
					eventBroker.send("stepChanged/syncEvent",viewer.getElementAt(viewer.getTable().getSelectionIndex()));
				}
				else{
					viewer.getTable().deselectAll();
					if(lastSelectionIndex!=-1){
						viewer.getTable().select(lastSelectionIndex);
					}
				}
			}

			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// TODO Auto-generated method stub
				
			}
		});
		this.stepSelectionController=new StepSelectionController(this);
	}
	public void setList(Vector<StepItf> steps){
		 viewer.setInput(steps);
		 parent.layout(true, true);
	}
	/**
	 *Updates the step list if an event indicates that the selected data types changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("dataTypeChanged/*") DataTypeItf selectedDataType) {
		if (selectedDataType != null) {
			  this.stepSelectionController.selectionChanged(selectedDataType);
			  this.viewer.getTable().select(0);
			  if(viewer.getTable().getSelectionIndex()!=-1){
				  eventBroker.send("stepChanged/syncEvent",viewer.getElementAt(viewer.getTable().getSelectionIndex()));
			  }
		  }
	} 
	/**
	 *Updates the step list if an event indicates that a new study was created
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("newStudy/*") String string) {
		if(string!=null){
			if(string.compareTo("new workspace")==0){
				this.viewer.setInput(new Vector<String>());
			}
		}
	} 
	/**
	 *Updates the step list if an event indicates that a step is finished
	 */
	@Inject
	void stepEventReceived(@Optional @UIEventTopic("stepDone/*") String string) {
		if(string!=null){
			if(string.compareTo("step done")==0){
				this.viewer.refresh();
			}
		}
	} 
}
