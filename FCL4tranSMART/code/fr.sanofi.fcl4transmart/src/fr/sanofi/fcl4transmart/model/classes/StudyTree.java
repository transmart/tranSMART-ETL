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

import java.util.Vector;

public class StudyTree {
	private TreeNode root;
	private boolean hasStudy;
	public StudyTree(TreeNode root){
		this.root=root;
		this.hasStudy=false;
	}
	public Object[] getRootToArray(){
		Vector<TreeNode> rootVector=new Vector<TreeNode>();
		rootVector.add(this.root);
		return rootVector.toArray();
	}
	public void setHasStudy(boolean bool){
		this.hasStudy=bool;
	}
	public boolean hasStudy(){
		return this.hasStudy;
	}
}
