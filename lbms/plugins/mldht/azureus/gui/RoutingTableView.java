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

import lbms.plugins.mldht.azureus.MlDHTPlugin;
import lbms.plugins.mldht.kad.DHT.DHTtype;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEvent;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEventListener;

/**
 * @author Damokles
 *
 */
public class RoutingTableView implements UISWTViewEventListener {

	public static final String	VIEWID			= "mldht_RoutingView";
	private static final int	UPDATE_INTERVAL	= 1000;

	private MlDHTPlugin			plugin;
	private boolean				isCreated		= false;
	private boolean				isActivated		= false;

	private RoutingTableCanvas	rtc;
	private Display				display;

	private Runnable			updateCanvas;
	private DHTtype				type;

	public RoutingTableView (MlDHTPlugin plugin, DHTtype type) {
		this.type = type;
		this.plugin = plugin;
		updateCanvas = new SWTSafeRunnable() {
			/* (non-Javadoc)
			 * @see lbms.plugins.mldht.azureus.gui.SWTSafeRunnable#runSafe()
			 */
			@Override
			public void runSafe () {
				if (!isActivated) {
					return;
				}
				rtc.fullRepaint();
				if (isActivated && display != null && !display.isDisposed()) {
					display.timerExec(UPDATE_INTERVAL, updateCanvas);
				}
			}
		};
	}

	/**
	 *
	 */
	private void delete () {
		deactivate();
	}

	/**
	 * @param data
	 */
	private void initialize (Composite data) {
		display = data.getDisplay();
		rtc = new RoutingTableCanvas(data);
	}

	/**
	 *
	 */
	private void deactivate () {
		if (!isActivated) {
			return;
		}
		//plugin.getDHT().removeStatsListener(dhtStatsListener);
		rtc.setNode(null);
		isActivated = false;
	}

	/**
	 *
	 */
	private void activate () {
		if (!isCreated || isActivated) {
			return;
		}
		rtc.setNode(plugin.getDHT(type).getNode());
		if (display != null && !display.isDisposed()) {
			display.timerExec(UPDATE_INTERVAL, updateCanvas);
		}

		isActivated = true;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.ui.swt.plugins.UISWTViewEventListener#eventOccurred(org.gudy.azureus2.ui.swt.plugins.UISWTViewEvent)
	 */
	public boolean eventOccurred (UISWTViewEvent event) {
		switch (event.getType()) {

		case UISWTViewEvent.TYPE_CREATE:
			if (isCreated) {
				return false;
			}
			isCreated = true;
			break;

		case UISWTViewEvent.TYPE_FOCUSGAINED:
			activate();
			break;

		case UISWTViewEvent.TYPE_FOCUSLOST:
			deactivate();
			break;

		case UISWTViewEvent.TYPE_INITIALIZE:
			initialize((Composite) event.getData());
			break;

		case UISWTViewEvent.TYPE_CLOSE:
		case UISWTViewEvent.TYPE_DESTROY:
			delete();
			break;
		}
		return true;
	}
}
