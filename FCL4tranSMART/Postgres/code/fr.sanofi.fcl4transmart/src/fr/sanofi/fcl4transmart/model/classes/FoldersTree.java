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
 *This class represents a study tree
 */	
public class FoldersTree {
	private Vector<FolderNode> roots;
	public FoldersTree(){
		this.roots=new Vector<FolderNode>();
	}
	public void addRoot(FolderNode root){
		this.roots.add(root);
	}
	public Object[] getRootToArray(){
		return this.roots.toArray();
	}
	public Vector<FolderNode> getRoots(){
		return this.roots;
	}
}
