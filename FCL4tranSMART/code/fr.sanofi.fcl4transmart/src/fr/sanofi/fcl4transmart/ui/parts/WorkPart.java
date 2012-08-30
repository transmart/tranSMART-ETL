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
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;

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
	@Inject
	void eventReceived(@Optional @UIEventTopic("preferencesChanged/*") String string) {
		 if (string != null && this.selectedStep != null) {
			 this.child.dispose();
			 this.workPartController.selectionChanged(this.selectedStep, this.parent);
		 }
	} 
	public void changeChild(Composite child){
		if(child!=null){
			this.child=child;
			GridData data = new GridData(GridData.FILL_BOTH);	    
			data.horizontalSpan = 1;	    
			this.child.setLayoutData(data);		    
			this.parent.layout(true, true);
		}
	}
	public static void updateSteps(){
		eventBroker.send("stepDone/syncEvent","step done");
	}
	public static void updateFiles(){
		eventBroker.send("fileUpdated/syncEvent", "");
	}
	public static Display display(){
		return display;
	}
}
