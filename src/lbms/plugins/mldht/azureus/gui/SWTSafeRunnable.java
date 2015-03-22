/*
 *    This file is part of mlDHT. 
 * 
 *    mlDHT is free software: you can redistribute it and/or modify 
 *    it under the terms of the GNU General Public License as published by 
 *    the Free Software Foundation, either version 2 of the License, or 
 *    (at your option) any later version. 
 * 
 *    mlDHT is distributed in the hope that it will be useful, 
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *    GNU General Public License for more details. 
 * 
 *    You should have received a copy of the GNU General Public License 
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>. 
 */
package lbms.plugins.mldht.azureus.gui;

import org.eclipse.swt.SWTException;

/**
 * This class is supposed to be used in asyncExec calls
 * in to SWT, runSafe will catch SWTExceptions and allow
 * the program to continue
 * 
 * @author Damokles
 *
 */
public abstract class SWTSafeRunnable implements Runnable {

	final public void run () {
		try {
			runSafe();
		} catch (SWTException e) {
			//Do Nothing
			e.printStackTrace();
		} catch (NullPointerException e) {
			//Notify
			e.printStackTrace();
		}
	}

	/**
	 * Run Safe catches SWTExceptions and NPE
	 */
	public abstract void runSafe();
}
