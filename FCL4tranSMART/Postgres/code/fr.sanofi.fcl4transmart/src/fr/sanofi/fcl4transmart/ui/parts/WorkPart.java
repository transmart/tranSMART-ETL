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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.core.services.events.IEventBroker;
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import fr.sanofi.fcl4transmart.controllers.WorkPartController;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
/**
 *This class handles the work part
 */
public class WorkPart {
	private WorkPartController workPartController;
	@Inject private static IEventBroker eventBroker;
	private Composite parent;
	private Composite child;
	private StepItf selectedStep;
	@Inject private static Display display;
	@PostConstruct
	public void createControls(Composite parent){
		this.parent=parent;
		GridLayout gd=new GridLayout();
		gd.numColumns=1;
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		this.parent.setLayout(gd);
		this.child=new Composite(parent, SWT.NONE);
		this.workPartController=new WorkPartController(this);
	}
	/**
	 *Updates the work part if an event indicates that the selected step has changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("stepChanged/*") StepItf selectedStep) {
		 if (selectedStep != null) {
			 this.selectedStep=selectedStep;
			 for(Control childControl: this.parent.getChildren()){
				 childControl.dispose();
			 }
			 this.workPartController.selectionChanged(selectedStep, this.parent);
		 }
	} 
	/**
	 *Updates the work part if an event indicates that the preferences were changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("preferencesChanged/*") String string) {
		 if (string != null && this.selectedStep != null) {
			 this.child.dispose();
			 this.workPartController.selectionChanged(this.selectedStep, this.parent);
		 }
	} 
	/**
	 *Add a new child composite to the work part
	 *Since version 1.2: before adding a composite, it is checked that there is no other one (for the case where work interfaces are long to calculate and the user selects another part to display before the first have been displayed)
	 */
	public void changeChild(Composite child){
		//check that no other work part has been added during calculation
		boolean hasChild=false;
		for(Control c: this.parent.getChildren()){
			if(c!=child){
				if(!c.isDisposed()){
					hasChild=true;
				}
			}
		}
		
		if(child!=null && !hasChild){
			this.child=child;
			GridData data = new GridData(GridData.FILL_BOTH);	    
			data.horizontalSpan = 1;	    
			this.child.setLayoutData(data);		    
			this.parent.layout(true, true);
		}
	}
	/**
	 *Send an event to indicate that steps have to be updated
	 */
	public static void updateSteps(){
		eventBroker.send("stepDone/syncEvent","step done");
	}
	/**
	 *Send an event to indicate that files have to be updated
	 */
	public static void updateFiles(){
		eventBroker.send("fileUpdated/syncEvent", "");
	}
	public static void addFiles(DataTypeItf selectedDataType){
		eventBroker.send("filesChanged/syncEvent",selectedDataType);
	}
	public static void updateAll(){
		eventBroker.send("preferencesChanged/syncEvent", "Preferences changed");
	}
	public static Display display(){
		return display;
	}
}