/**
 * 
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
