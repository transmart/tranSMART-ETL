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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.Viewer;

import fr.sanofi.fcl4transmart.model.classes.StudyTree;
import fr.sanofi.fcl4transmart.model.classes.TreeNode;
/**
 *This class represents a tree node
 */	
public class StudyContentProvider implements ITreeContentProvider{
	private StudyTree studyTree;
	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
		// TODO Auto-generated method stub
		this.studyTree=(StudyTree)newInput;	
	}

	@Override
	public Object[] getElements(Object inputElement) {
		// TODO Auto-generated method stub
		return this.studyTree.getRootToArray();
	}

	@Override
	public Object[] getChildren(Object parentElement) {
		return ((TreeNode)parentElement).getChildren().toArray();
	}

	@Override
	public Object getParent(Object element) {
		// TODO Auto-generated method stub
		return ((TreeNode)element).getParent();
	}

	@Override
	public boolean hasChildren(Object element) {
		if(((TreeNode)element).getChildren().size()>0){
			return true;
		}
		return false;
	}
}
