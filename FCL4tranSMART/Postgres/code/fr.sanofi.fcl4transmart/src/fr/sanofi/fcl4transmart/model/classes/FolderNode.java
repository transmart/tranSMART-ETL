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
public class FolderNode {
	Vector<FolderNode> children;
	FolderNode parent;
	private int folderId;
	private String folderName;
	private String folderFullName;
	private int folderLevel;
	private String objectUid;
	private String folderType;
	private String folderTag;
	public FolderNode(FolderNode parent){
		this.parent=parent;
		this.children=new Vector<FolderNode>();
	}
	public void addChild(FolderNode child){
		this.children.add(child);
	}
	public String toString(){
		return this.folderName;
	}
	public Vector<FolderNode> getChildren(){
		return this.children;
	}
	public FolderNode getParent(){
		return this.parent;
	}
	public boolean hasChildren(){
		return (this.children.size()>0);
	}
	public void removeChild(FolderNode child){
		this.children.remove(child);
	}
	
	public FolderNode getChild(String name){
		for(FolderNode child: this.children){
			if(child.toString().compareTo(name)==0){
				return child;
			}
		}
		return null;
	}
	public void setId(int id){
		this.folderId=id;
	}
	public void setName(String name){
		this.folderName=name;
	}
	public void setFullName(String fullName){
		this.folderFullName=fullName;
	}
	public void setLevel(int level){
		this.folderLevel=level;
	}
	public void setObjectUid(String uid){
		this.objectUid=uid;
	}
	public void setType(String type){
		this.folderType=type;
	}
	public void setTag(String tag){
		this.folderTag=tag;
	}
	public int getId(){
		return this.folderId;
	}
	public String getName(){
		return this.folderName;
	}
	public String getFullName(){
		return this.folderFullName;
	}
	public int getLevel(){
		return this.folderLevel;
	}
	public String getObjectUid(){
		return this.objectUid;
	}
	public String getType(){
		return this.folderType;
	}
	public String getTag(){
		return this.folderTag;
	}
}
