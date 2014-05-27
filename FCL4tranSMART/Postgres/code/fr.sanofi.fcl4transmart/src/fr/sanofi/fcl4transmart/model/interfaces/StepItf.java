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

public interface StepItf {
	/**
	 *Returns the work zone object associated with a step
	 */
	public WorkItf getWorkUI();
	/**
	 *Returns a string containing description to display in the 'step description' part
	 */
	public String getDescription();
	/**
	 *Returns a boolean indicating if a step is available to be selected by the user
	 */
	public boolean isAvailable();
}
