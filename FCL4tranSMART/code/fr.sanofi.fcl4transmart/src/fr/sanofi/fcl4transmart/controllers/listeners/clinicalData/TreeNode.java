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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

import java.util.Vector;

public class TreeNode {
	public String name;
	Vector<TreeNode> children;
	TreeNode parent;
	boolean isLabel;
	public TreeNode(String name, TreeNode parent, boolean isLabel){
		this.name=name;
		this.parent=parent;
		this.children=new Vector<TreeNode>();
		this.isLabel=isLabel;
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
		this.children.removeElement(child);
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
}
