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
package fr.sanofi.fcl4transmart.handlers;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class VersionHandler {
	@Execute
	public void execute(Display display) {
		Shell shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    shell.setSize(500,250);
	    shell.setText("Version");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		
		Label label=new Label(shell, SWT.CENTER);
		label.setText("Framework Curation & Loading For tranSMART\n"+
					"(FC&L4tranSMART)\n");
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		label.setLayoutData(gridData);
		
		Label label2=new Label(shell, SWT.NONE);
		label2.setText("Version 1.1\n"+
						"\n"+
						"This version has been tested with tranSMART version 1.0 RC2, with Java Runtime Environment 1.6 and 1.7.\n"+
						"\n"+
						"This application has been developed by Sanofi R&D IS unit, and is distributed under licence GPL3.");
		
		shell.open();
	    
	    while(!shell.isDisposed()){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }
	    }
	}
}
