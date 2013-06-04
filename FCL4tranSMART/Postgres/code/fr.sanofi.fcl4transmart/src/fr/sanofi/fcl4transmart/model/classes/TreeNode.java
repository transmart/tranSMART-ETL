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
package fr.sanofi.fcl4transmart.model.classes;

import java.util.Vector;
/**
 *This class represents a node in a study tree. 
 *A node can be:
 *-free text
 *-a property of the raw data file (attribute isLabel set to true)
 *-an operation (attribute isOperation set to true)
 *-a study top node (attribute isStudyRoot set to true)
 */	
public class TreeNode {
	public String name;
	Vector<TreeNode> children;
	TreeNode parent;
	private boolean isLabel;
	private boolean isStudyRoot;
	//private boolean isOperation;
	public TreeNode(String name, TreeNode parent, boolean isLabel){
		this.name=name;
		this.parent=parent;
		this.children=new Vector<TreeNode>();
		this.isLabel=isLabel;
		this.isStudyRoot=false;
		//this.isOperation=false;
	}
	public void addChild(TreeNode child){
		this.children.add(child);
	}
	public String toString(){
		return this.name;
	}
	public Vector<TreeNode> getChildren(){
		return this.children;
	}
	public TreeNode getParent(){
		return this.parent;
	}
	public boolean hasChildren(){
		return (this.children.size()>0);
	}
	public void removeChild(TreeNode child){
		this.children.remove(child);
	}
	public boolean isLabel(){
		return this.isLabel;
	}
	public TreeNode getChild(String name){
		for(TreeNode child: this.children){
			if(child.toString().compareTo(name)==0){
				return child;
			}
		}
		return null;
	}
	public boolean isStudyRoot(){
		return this.isStudyRoot;
	}
	public void setIsStudyTree(boolean bool){
		this.isStudyRoot=bool;
	}
	public void setName(String name){
		this.name=name;
	}
	/*public void setIsOperation(boolean bool){
		this.isOperation=bool;
	}
	public boolean isOperation(){
		return this.isOperation;
	}*/
}
