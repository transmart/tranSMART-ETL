/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et D�veloppement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et D�veloppement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.model.classes.dataType;

import java.io.File;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.CheckAnnotation;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.LoadData;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.Monitoring;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.QualityControl;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SelectRawFile;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SelectSTSMF;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetAttribute1;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetAttribute2;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetPlatforms;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetSiteId;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetStudyTree;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetSubjectsId;
import fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData.SetTissueType;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
/**
 *This class handles the gene exoression data loading. It contains the paths to the folder representing gene expression data for a study, and paths to:
 *-raw data file
 *-subject to sample mapping file
 *-Kettle log file for gene expression loading
 *-Kettle log file for platform annotation loading
 * The list of steps required for gene expression data loading are set in this class
 */	
public class GeneExpressionData implements DataTypeItf{
	private Vector<StepItf> steps;
	private Vector<File> rawFiles;
	private File stsmf;//subject to sample mapping file
	private File logFile;
	private File annotationLogFile;
	private StudyItf study; 
	private File path;
	public GeneExpressionData(StudyItf study){
		this.study=study;
		this.steps=new Vector<StepItf>();
		
		//add the different steps here
		this.steps.add(new SelectRawFile(this));
		this.steps.add(new SelectSTSMF(this));
		this.steps.add(new SetSubjectsId(this));
		this.steps.add(new SetPlatforms(this));
		this.steps.add(new SetTissueType(this));
		this.steps.add(new SetSiteId(this));
		this.steps.add(new SetAttribute1(this));
		this.steps.add(new SetAttribute2(this));
		this.steps.add(new SetStudyTree(this));
		this.steps.add(new CheckAnnotation(this));
		this.steps.add(new LoadData(this));
		this.steps.add(new Monitoring(this));
		this.steps.add(new QualityControl(this));
	}
	@Override
	public Vector<StepItf> getSteps() {
		return this.steps;
	}
	public String toString(){
		return "Gene expression data";
	}
	public void setFiles(File path){
		this.path=path;
		File[] children=this.path.listFiles();
		this.rawFiles=new Vector<File>();
		Pattern patternSTSMF=Pattern.compile(".*\\.subject_mapping");
		Pattern patternRaw=Pattern.compile("raw\\..*");
		for(int i=0; i<children.length;i++){
			if(children[i].isFile()){
				Matcher matcherSTSMF=patternSTSMF.matcher(children[i].getName());
				if(matcherSTSMF.matches()){
					this.stsmf=children[i];
				}else if(children[i].getName().compareTo("kettle.log")==0){
					this.logFile=children[i];
				}else if(children[i].getName().compareTo("annotation.kettle.log")==0){
					this.annotationLogFile=children[i];
				}
				else{
					Matcher matcherRaw=patternRaw.matcher(children[i].getName());
					if(!matcherRaw.matches()){
						children[i].renameTo(new File(this.path+File.separator+"raw."+children[i].getName()));
					}
					this.rawFiles.add(children[i]);
				}
			}
		}
	}
	public Vector<File> getFiles(){
		Vector<File> v=new Vector<File>();
		if(this.rawFiles!=null){
			v.addAll(this.rawFiles);
		}
		if(this.stsmf!=null){
			v.add(this.stsmf);
		}
		if(this.logFile!=null){
			v.add(this.logFile);
		}
		if(this.annotationLogFile!=null){
			v.add(this.annotationLogFile);
		}
		return v;
	}	
	public StudyItf getStudy(){
		return this.study;
	}
	public File getPath(){
		return this.path;
	}
	public Vector<File> getRawFiles(){
		return this.rawFiles;
	}
	public File getStsmf(){
		return this.stsmf;
	}
	public Vector<String> getRawFilesNames(){
		Vector<String> rawFilesNames=new Vector<String>();
		for(File f: this.rawFiles){
			rawFilesNames.add(f.getName());
		}
		return rawFilesNames;
	}
	public void addRawFile(File rawFile){
		this.rawFiles.add(rawFile);
	}
	public void setSTSMF(File file){
		this.stsmf=file;
	}
	public File getLogFile(){
		return this.logFile;
	}
	public void setLogFile(File logFile){
		this.logFile=logFile;
	}
	public File getAnnotationLogFile(){
		return this.annotationLogFile;
	}
	public void setAnnotationLogFile(File logFile){
		this.annotationLogFile=logFile;
	}
}
