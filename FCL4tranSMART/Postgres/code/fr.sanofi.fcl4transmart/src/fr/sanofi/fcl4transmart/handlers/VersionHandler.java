/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis R&D.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Sanofi-Aventis R&D - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.handlers;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
/**
 *This class controls the version window called from menu
 */	
public class VersionHandler {
	@Execute
	public void execute(Display display) {
		Shell shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    shell.setSize(500,470);
	    shell.setText("Version");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		
		Label label=new Label(shell, SWT.CENTER);
		label.setText("Framework Curation and Loading For tranSMART\n");
		GridData gridData = new GridData();
		gridData.widthHint=480;
		gridData.grabExcessHorizontalSpace = true;
		label.setLayoutData(gridData);
		
		Label label2=new Label(shell, SWT.WRAP);
		gridData = new GridData();
		gridData.widthHint=480;
		gridData.grabExcessHorizontalSpace = true;
		label2.setLayoutData(gridData);
		label2.setText("Version 1.1\n"+
						"\n"+
						"This version has been tested with tranSMART version 1.0 RC2, with Java Runtime Environment 1.6 and 1.7.\n"+
						"\n"+
						"This application has been developed by Sanofi-Aventis Recherche et Développement, and is distributed under GNU General Public License version 3."+
						"\n\n"+
						"Framework of Curation and Loading for tranSMART - Version 1.1\n"+  
						"Copyright (C) 2012 Sanofi-Aventis Recherche et Développement\n\n"+
						"This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.\n\n"+
						"This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.\n\n"+
						"You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.\n\n"+
						"Additional terms are applicable to FC&L4tranSMART, in accordance with section 7 of the GNU General Public License version 3:\n"+
						"YOU EXPRESSLY UNDERSTAND AND AGREE THAT YOUR USE OF THE SOFTWARE IS AT YOUR SOLE RISK AND THAT THE SOFTWARE IS PROVIDED \" AS IS \" AND \" AS AVAILABLE. \" . THERE IS NO WARRANTY THAT \n "+
						"(I)	YOUR USE OF THE PROGRAM WILL MEET YOUR REQUIREMENTS,\n"+
						"(II)	YOUR USE OF THE PROGRAM WILL BE FREE FROM ERROR,\n"+
						"(III)	ANY INFORMATION OBTAINED BY YOU AS A RESULT OF YOUR USE OF THE PROGRAM WILL BE ACCURATE OR RELIABLE.");
		
		shell.setSize(shell.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		shell.open();
	    
	    while(!shell.isDisposed()){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }
	    }
	}
}
