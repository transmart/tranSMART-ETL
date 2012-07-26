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
package fr.sanofi.fcl4transmart.model.classes;

import java.io.File;
import java.util.Vector;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.dataType.StudyDescription;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;

public class Study implements StudyItf{
	private String name;
	private Vector<DataTypeItf> dataTypes;
	private File path;
	private boolean[] areFoldersPresent;
	@SuppressWarnings("unused")
	private String topNode;//needed by all data types, should be automatically set for other data types when it has been indicated for one
	public Study(String name, File path){
		this.dataTypes=new Vector<DataTypeItf>();
		this.name=name;
		this.path=path;
		this.areFoldersPresent=new boolean[3];
		for(int i=0; i<3; i++){
			this.areFoldersPresent[i]=false;
		}
		this.dataTypes.add(new StudyDescription(this));
		this.dataTypes.add(new ClinicalData(this));
		this.dataTypes.add(new GeneExpressionData(this));
		this.setDataTypesPaths();

	}
	@Override
	public String toString(){
		return this.name;
	}
	public Vector<DataTypeItf> getDataTypes(){
		return this.dataTypes;
	}
	public Vector<String> getMissingFolders(){
		Vector<String> missingFolders=new Vector<String>();
		if(!this.areFoldersPresent[0]){
			missingFolders.add("description");
		}
		if(!this.areFoldersPresent[1]){
			missingFolders.add("clinical");
		}
		if(!this.areFoldersPresent[2]){
			missingFolders.add("gene");
		}
		return missingFolders;
	}
	public void setName(String name){
		this.name=name;
	}
	public File getPath(){
		return this.path;
	}
	public void setPath(File newPath){
		this.path=newPath;
		this.setDataTypesPaths();
	}
	public void setDataTypesPaths(){
		File[] children=this.path.listFiles();
		for(int i=0; i<children.length; i++){
			if(children[i].isDirectory()){
				if(children[i].getName().compareTo("description")==0){
					this.dataTypes.get(0).setFiles(children[i]);
					this.areFoldersPresent[0]=true;
				}else if(children[i].getName().compareTo("clinical")==0){
					this.dataTypes.get(1).setFiles(children[i]);
					this.areFoldersPresent[1]=true;
				}else if(children[i].getName().compareTo("gene")==0){
					this.dataTypes.get(2).setFiles(children[i]);
					this.areFoldersPresent[2]=true;
				}
			}
		}
	}
}
