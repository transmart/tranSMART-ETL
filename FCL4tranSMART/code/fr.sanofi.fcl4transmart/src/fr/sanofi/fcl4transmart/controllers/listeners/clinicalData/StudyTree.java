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

public class StudyTree {
	private TreeNode root;
	public StudyTree(TreeNode root){
		this.root=root;
	}
	public Object[] getRootToArray(){
		Vector<TreeNode> rootVector=new Vector<TreeNode>();
		rootVector.add(this.root);
		return rootVector.toArray();
	}
}
