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
package fr.sanofi.fcl4transmart.model.interfaces;

import java.util.Vector;

import org.eclipse.swt.widgets.Composite;

public interface WorkItf {
	/**
	 *Returns the interface to display in the work part for a step
	 */
	public Composite createUI(Composite parent);
	/**
	 *Returns true if the work interface has columns to copy, false otherwise
	 */
	public boolean canCopy();
	/**
	 *Returns true if the work interface has columns where to paste, false otherwise
	 */
	public boolean canPaste();
	/**
	 *Copy data from the work interface fields in vectors
	 */
	public Vector<Vector<String>> copy();
	/**
	 *Past data in vectors in the work interface fields
	 */
	public void paste(Vector<Vector<String>> data); 
	public void mapFromClipboard(Vector<Vector<String>> data);
}
