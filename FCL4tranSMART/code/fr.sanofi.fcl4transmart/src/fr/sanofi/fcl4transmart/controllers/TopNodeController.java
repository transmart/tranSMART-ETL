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
package fr.sanofi.fcl4transmart.controllers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Vector;

import fr.sanofi.fcl4transmart.model.classes.StudyTree;
import fr.sanofi.fcl4transmart.model.classes.TreeNode;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;

public class TopNodeController {
	private TreeNode root;
	private DataTypeItf dataType;
	private StudyTree tree;
	public TopNodeController(TreeNode root, DataTypeItf dataType, StudyTree tree){
		this.root=root;
		this.dataType=dataType;
		this.tree=tree;
	}
	public TreeNode buildTree(){
		if(!(RetrieveData.testDemodataConnection() && RetrieveData.testMetadataConnection())){
			this.root.setName("");
			return this.root;
		}
		
		Vector<String> studies=RetrieveData.getStudies();
		for(String study: studies){
			this.addNode(this.root, study, 1);
		}
		if(!this.tree.hasStudy()){
			String topNode=this.dataType.getStudy().getTopNode();
			if(topNode!=null && topNode.compareTo("")!=0){
				for(TreeNode topFolder: this.root.getChildren()){
					if(topFolder.toString().compareTo(topNode.split("\\\\", -1)[1])==0){
						this.addThisStudy(topFolder, topNode, 2);
					}
				}
				if(!this.tree.hasStudy()){
					TreeNode newTopFolder=new TreeNode(topNode.split("\\\\", -1)[1], this.root, false);
					this.root.addChild(newTopFolder);
					this.addThisStudy(newTopFolder, topNode, 2);
				}
			}
		}
		/*
		//get all sub-folders
		Vector<String> subFolders=RetrieveData.getSubFolders();
		//create top folders
		for(String topFolder: RetrieveData.getTopFolders()){
			TreeNode topNode=new TreeNode(topFolder, this.root, false);
			this.root.addChild(topNode);
			for(String subFolder: subFolders){
				if(subFolder.split("\\\\", -1)[1].compareTo(topFolder)==0){
					this.addFolders(topNode, subFolder, 2);
				}
			}
			//for each node, search if a study is a child
			this.addStudies(topNode, "\\");
			
		}
		if(!this.tree.hasStudy()){
			String topNode=this.dataType.getStudy().getTopNode();
			if(topNode!=null && topNode.compareTo("")!=0){
				for(TreeNode topFolder: this.root.getChildren()){
					if(topFolder.toString().compareTo(topNode.split("\\\\", -1)[1])==0){
						this.addThisStudy(topFolder, topNode, 2);
					}
				}
				if(!this.tree.hasStudy()){
					TreeNode newTopFolder=new TreeNode(topNode.split("\\\\", -1)[1], this.root, false);
					this.root.addChild(newTopFolder);
					this.addThisStudy(newTopFolder, topNode, 2);
				}
			}
		}*/
		return this.root;
	}
	/*public void addFolders(TreeNode node, String pathToAdd, int n){
		if(pathToAdd.split("\\\\", -1).length<4) return;
		String nodeName=pathToAdd.split("\\\\", -1)[n];
		TreeNode child=node.getChild(nodeName);
		if(child==null){
			child=new TreeNode(nodeName, node, false);
			node.addChild(child);
		}
		if(n<pathToAdd.split("\\\\", -1).length-2){
			this.addFolders(child, pathToAdd, n+1);
		}
	}*/
	public void addNode(TreeNode node, String pathToAdd, int n){
		if(pathToAdd.split("\\\\", -1).length<4) return;
		String nodeName=pathToAdd.split("\\\\", -1)[n];
		TreeNode child=node.getChild(nodeName);
		if(child==null){
			if(n==pathToAdd.split("\\\\", -1).length-2){
				child=new TreeNode(nodeName, node, true);
				if(RetrieveData.getIdFromPath(pathToAdd).compareTo(this.dataType.getStudy().toString().toUpperCase())==0){
					if(this.dataType.getStudy().getTopNode()==null || this.dataType.getStudy().getTopNode().compareTo("")==0){
						child.setIsStudyTree(true);
						node.addChild(child);
						tree.setHasStudy(true);
						this.dataType.getStudy().setTopNode(pathToAdd);
					}
				}
				else{
					node.addChild(child);
				}
			}
			else{
				child=new TreeNode(nodeName, node, false);
				node.addChild(child);
			}
		}
		if(n<pathToAdd.split("\\\\", -1).length-2){
			this.addNode(child, pathToAdd, n+1);
		}
	}
	public void addThisStudy(TreeNode node, String path, int n){
		if(path.split("\\\\", -1).length<4) return;
		String nodeName=path.split("\\\\", -1)[n];
		if(n==path.split("\\\\", -1).length-2){
			TreeNode study=new TreeNode(path.split("\\\\", -1)[path.split("\\\\", -1).length-2], node, true);
			study.setIsStudyTree(true);
			node.addChild(study);
			this.tree.setHasStudy(true);
		}
		else{
			TreeNode child=node.getChild(nodeName);
			if(child==null){
				child=new TreeNode(nodeName, node, false);
				node.addChild(child);
			}
			this.addThisStudy(child, path, n+1);
		}
	}
	/*
	public void addStudies(TreeNode node, String path){//for the root, put '\' as the path
		path+=node.toString()+"\\";
		for(TreeNode child: node.getChildren()){
			addStudies(child, path);
		}
		Vector<String> studies=RetrieveData.getStudies(path);
		for(String study: studies){
			String name=study.split("\\\\", -1)[study.split("\\\\", -1).length-2];
			TreeNode newNode;
			if(RetrieveData.getIdFromPath(study).compareTo(this.dataType.getStudy().toString().toUpperCase())==0){
				if(this.dataType.getStudy().getTopNode()==null || this.dataType.getStudy().getTopNode().compareTo("")==0){
					newNode=new TreeNode(name, node, true);
					newNode.setIsStudyTree(true);
					tree.setHasStudy(true);
					node.addChild(newNode);
					this.dataType.getStudy().setTopNode(path+name+"\\");
				}
			}
			else{
				newNode=new TreeNode(name, node, true);
				node.addChild(newNode);
			}
		}
	}*/
	public String getTopNode(TreeNode root){
		for(TreeNode topFolder: root.getChildren()){
			String path=this.getPath(topFolder, "\\");
			if(path.compareTo("")!=0){
				return path;
			}
		}
		return "";
	}
	public String getPath(TreeNode node, String path){
		path+=node.toString()+"\\";
		String topNode="";
		for(TreeNode child: node.getChildren()){
			if(child.isStudyRoot()){
				return path+child.toString()+"\\";
			}
			topNode=this.getPath(child, path);
		}
		return topNode;
	}
	public void writeTopNode(){
		String topNode=this.getTopNode(this.root);
		if(topNode!=null){
			try{
				File file=new File(this.dataType.getStudy().getPath()+File.separator+".top_node");
				FileWriter fw = new FileWriter(file);
				BufferedWriter out = new BufferedWriter(fw);
				out.write(topNode);
				out.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			this.dataType.getStudy().setTopNode(topNode);
		}
	}
	public static String readTopNode(File file){
		String line;
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			line=br.readLine();
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
		return line;
	}
}
