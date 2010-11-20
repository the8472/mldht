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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import lbms.plugins.mldht.azureus.MlDHTPlugin;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.utils.PopulationListener;

import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.gudy.azureus2.plugins.ui.UIInstance;
import org.gudy.azureus2.plugins.ui.UIManagerListener;
import org.gudy.azureus2.ui.swt.plugins.UISWTInstance;
import org.gudy.azureus2.ui.swt.plugins.UISWTStatusEntry;

/**
 * @author Leonard
 * 
 */
public class SWTHelper implements UIManagerListener, PopulationListener, DHTStatusListener {
	private UISWTStatusEntry	statusEntry;
	private UISWTInstance		swtInstance;
	private MlDHTPlugin			plugin;
	private List<DHTView>		views = new ArrayList<DHTView>();

	public Image				dhtStatusEntryIcon;
	public Display				display;

	public SWTHelper(MlDHTPlugin plugin) {
		this.plugin = plugin;
	}

	public void onPluginUnload () {
		
		for(DHTtype type : DHTtype.values()) {
			swtInstance.removeViews(UISWTInstance.VIEW_MAIN, DHTView.VIEWID+"."+type.shortName);
		}
		
		if (statusEntry != null) {
			statusEntry.destroy();
			statusEntry = null;
		}
		
		if (dhtStatusEntryIcon != null) {
			dhtStatusEntryIcon.dispose();
			dhtStatusEntryIcon = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.gudy.azureus2.plugins.ui.UIManagerListener#UIAttached(org.gudy.azureus2.plugins.ui.UIInstance)
	 */
	public void UIAttached (UIInstance instance) {

		if (instance instanceof UISWTInstance) {
			swtInstance = (UISWTInstance) instance;
			display = swtInstance.getDisplay();

			try {
				statusEntry = swtInstance.createStatusEntry();
				dhtStatusEntryIcon = new Image(
						swtInstance.getDisplay(),
						MlDHTPlugin.class
								.getResourceAsStream("/lbms/plugins/mldht/azureus/gui/dhtIcon.png"));
				statusEntry.setImage(dhtStatusEntryIcon);
				statusEntry.setImageEnabled(true);
			} catch (RuntimeException e) {
				e.printStackTrace();
			}

			
			for(DHTtype type : DHTtype.values())
			{
				plugin.getDHT(type).getEstimator().addListener(this);
				plugin.getDHT(type).addStatusListener(this);
				DHTView view = new DHTView(plugin, swtInstance.getDisplay(),type);
				views.add(view);
				swtInstance.addView(UISWTInstance.VIEW_MAIN, DHTView.VIEWID+"."+type.shortName, view);
				if (plugin.isPluginAutoOpen(type.shortName)) {
					swtInstance.openMainView(DHTView.VIEWID+"."+type.shortName, view, null);
				}

			}

		}
	}

	public void UIDetached (UIInstance instance) {
		if (instance instanceof UISWTInstance) {
			swtInstance = null;
			display = null;
			views.clear();
			for(DHTtype type : DHTtype.values())
			{
				plugin.getDHT(type).getEstimator().removeListener(this);
				plugin.getDHT(type).removeStatusListener(this);
			}

		}
	}
	
	private void updateStatusEntry()
	{

		if (statusEntry != null) {
			DecimalFormat	format	= new DecimalFormat();
			
			final StringBuilder text = new StringBuilder("mlDHT: ");
			final StringBuilder tooltip = new StringBuilder("Node Estimate:");
			for(DHTtype type : DHTtype.values())
			{
				DHT dht = plugin.getDHT(type);
				DHTStatus status = dht.getStatus();
				text.append(" "+type.shortName+": ");
				tooltip.append(" "+type.shortName+": ");
				if(status == DHTStatus.Running && dht.getEstimator().getEstimate() != 0)
				{
					text.append("\u2714");
					tooltip.append(format.format(dht.getEstimator().getEstimate()));
				} else {
					text.append("\u2718");
					tooltip.append(status);
				}
				
			}

			if (display != null && !display.isDisposed()) {
				display.asyncExec(new Runnable() {
					public void run () {
						statusEntry.setText(text.toString());
						statusEntry.setTooltipText(tooltip.toString());
						statusEntry.setVisible(plugin.getPluginInterface().getPluginconfig().getPluginBooleanParameter("showStatusEntry"));
					}
				});
			}

			
		}
		
	}
	
	public void statusChanged(DHTStatus newStatus, DHTStatus oldStatus) {
		updateStatusEntry();
	}
	
	@Override
	public void populationUpdated(long estimatedPopulation) {
		updateStatusEntry();
	}
}
