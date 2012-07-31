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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.ui.di.UIEventTopic;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

public class FilesViewerPart {
	private Text editor;
	public FilesViewerPart(){
	}
	@PostConstruct
	public void createControls(Composite parent){
		GridLayout gd = new GridLayout();
		gd.horizontalSpacing=0;
		gd.verticalSpacing=0;
		parent.setLayout(gd);
		this.editor=new Text(parent, SWT.BORDER|SWT.V_SCROLL|SWT.H_SCROLL|SWT.WRAP);
		this.editor.setLayoutData(new GridData(GridData.FILL_BOTH));
		this.editor.setEditable(false);
	}
	@Inject
	void eventReceived(@Optional @UIEventTopic("fileChanged/*") File file) {
		if (file != null) {
			this.changeText(file);
		  }
	} 
	public void changeText(File file){
		if(file==null) return;
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			String text="";
			int cnt=0;
			while((line=br.readLine())!=null && cnt<=500){
				cnt++;
				if(cnt==500){
					text+="\nFile too long";
					br.close();
					this.editor.setText(text);
					return;
				}
				text+=line;
				text+="\n";
			}
			br.close();
			this.editor.setText(text);
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
