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
/**
 *This class handles the file viewer part
 */
public class FilesViewerPart {
	private Text editor;
	private File lastFile;
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
	/**
	 *Changes the displayed file is another one is selected in the file viewer list
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("fileChanged/*") File file) {
		this.lastFile=file;
		this.changeText();
	} 
	/**
	 *Updates the displayed files if an event indicates it changed
	 */
	@Inject
	void eventReceived(@Optional @UIEventTopic("fileUpdated/*") String s) {
		this.changeText();
	} 
	public void changeText(){
		if(this.editor==null) return;//to avoid an error at launching

		if(this.lastFile==null){
			try{
				this.editor.setText("");
				return;
			}catch(Exception e){
				return;
			}
		}
		try{
			BufferedReader br = new BufferedReader(new FileReader(this.lastFile));
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
		}
	}
}
