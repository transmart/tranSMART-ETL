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
package fr.sanofi.fcl4transmart.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import fr.sanofi.fcl4transmart.controllers.listeners.clinicalData.TreeNode;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;

public class StudyTreeController {
	private TreeNode root;
	private DataTypeItf dataType;
	public StudyTreeController(TreeNode root, DataTypeItf dataType){
		this.root=root;
		this.dataType=dataType;
	}
	public TreeNode buildTree(File file){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", 40);
				File rawFile=new File(this.dataType.getPath()+File.separator+s[0]);
				String header=FileHandler.getColumnByNumber(rawFile, Integer.parseInt(s[2]));
				String name=s[0]+" - "+header;
				if(s[3].compareTo("\\")==0){
					String sourceName=rawFile.getName()+" - "+FileHandler.getColumnByNumber(rawFile, Integer.parseInt(s[4]));
					this.buildNode(this.root, s[1], name, sourceName);
				}
				else if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("SUBJ_ID")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0){
					this.buildNode(this.root, s[1], name, "");
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		
		return this.root;
	}
	public void buildNode(TreeNode node, String path, String label, String dataLabelSource){
		String[] splitedPath=path.split("\\+",2);
		TreeNode child=node.getChild(splitedPath[0].replace('_', ' '));
		if(child==null){
			child=new TreeNode(splitedPath[0].replace('_', ' '), node, false);
			node.addChild(child);
		}
		if(splitedPath.length>1){
			this.buildNode(child, splitedPath[1], label, dataLabelSource);
		}
		else{
			if(dataLabelSource.compareTo("")==0){
				if(child.getChild(label)==null){
					child.addChild(new TreeNode(label, child, true));
				}
			}
			else{
				if(child.getChild(dataLabelSource)!=null){
					if(child.getChild(dataLabelSource).getChild(label)==null){
						child.getChild(dataLabelSource).addChild(new TreeNode(label, child.getChild(dataLabelSource), true));
					}
				}
			}
		}
	}
}
