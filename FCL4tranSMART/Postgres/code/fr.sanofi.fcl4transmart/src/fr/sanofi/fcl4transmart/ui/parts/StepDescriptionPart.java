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
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import fr.sanofi.fcl4transmart.controllers.StepDescriptionController;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
/**
 *This class handles the step description part
 */
public class StepDescriptionPart {
	private Text text;
	private StepDescriptionController stepDescriptionController;
	@PostConstruct
	public void createControls(Composite parent){
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		parent.setLayout(gd);
		this.text=new Text(parent, SWT.BORDER|SWT.V_SCROLL|SWT.H_SCROLL|SWT.WRAP);
		this.text.setEditable(false);
		this.text.setLayoutData(new GridData(GridData.FILL_BOTH));
		this.stepDescriptionController=new StepDescriptionController(this);
	}
	/**
	 *Updates the step description if an event indicates that the selected step changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("stepChanged/*") StepItf selectedStep) {
		 if (selectedStep != null) {
			 this.stepDescriptionController.selectionChanged(selectedStep);
		 }
	} 
	public void setDescription(String description){
		this.text.setText(description);
	}
	/**
	 *Remove the step description if an event indicates that a new study was created (no step selected)
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("newStudy/*") String string) {
		if(string!=null){
			if(string.compareTo("new workspace")==0){
				this.text.setText("");
			}
		}
	} 
}
