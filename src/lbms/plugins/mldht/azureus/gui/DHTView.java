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
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTStats;
import lbms.plugins.mldht.kad.DHTStatsListener;
import lbms.plugins.mldht.kad.RPCStats;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.tasks.*;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.gudy.azureus2.plugins.utils.Formatters;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEvent;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEventListener;

/**
 * @author Damokles
 * 
 */
public class DHTView implements UISWTViewEventListener {

	public static final String	VIEWID			= "mldht_View";
	public static final String	DONATION_URL	= "http://azsmrc.sourceforge.net/index.php?action=supportUs";

	private MlDHTPlugin			plugin;
	private boolean				isCreated		= false;
	private boolean				isActivated		= false;
	private boolean				isRunning		= false;
	private Formatters			formatters;

	private DHTStatsListener	dhtStatsListener;

	private Label				peerCount;
	private Label				taskCount;
	private Label				keysCount;
	private Label				itemsCount;
	private Label				sentPacketCount;
	private Label				receivedPacketCount;
	private Label				activeRPCCount;
	private Label				ourID;
	private Label				receivedBytesTotal;
	private Label				sentBytesTotal;
	private Label				receivedBytes;
	private Label				sentBytes;
	private Label				uptime;
	private Label				avgSentBytes;
	private Label				avgReceivedBytes;

	private Label				dhtRunStatus;
	private Label[][]			messageLabels;
	private Button				dhtStartStop;

	private Group				dhtStatsGroup;
	private Group				serverStatsGroup;
	private Group				messageStatsGroup;
	private RoutingTableCanvas	rtc;

	private Table				taskTable;
	private Task[]				tasks;
	private Image				donationImg;
	private final DHTtype		type;

	public DHTView(MlDHTPlugin _plugin, final Display display, DHTtype type) {
		this.plugin = _plugin;
		this.type = type;
		isRunning = plugin.getDHT(type).isRunning();
		formatters = plugin.getPluginInterface().getUtilities().getFormatters();
		dhtStatsListener = new DHTStatsListener() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see lbms.plugins.mldht.kad.DHTStatsListener#statsUpdated(lbms.plugins.mldht.kad.DHTStats)
			 */
			public void statsUpdated (final DHTStats stats) {
				if (!isCreated) {
					return;
				}
				if (display != null && !display.isDisposed()) {
					display.asyncExec(new SWTSafeRunnable() {
						/*
						 * (non-Javadoc)
						 * 
						 * @see lbms.plugins.mldht.azureus.gui.SWTSafeRunnable#runSafe()
						 */
						@Override
						public void runSafe () {
							if (!isActivated || peerCount == null
									|| peerCount.isDisposed()) {
								deactivate();
								return;
							}
							peerCount.setText(String.valueOf(stats
									.getNumPeers()));
							taskCount.setText(String.valueOf(stats
									.getNumTasks()));
							keysCount.setText(String.valueOf(stats.getDbStats()
									.getKeyCount()));
							itemsCount.setText(String.valueOf(stats
									.getDbStats().getItemCount()));
							sentPacketCount.setText(String.valueOf(stats
									.getNumSentPackets()));
							receivedPacketCount.setText(String.valueOf(stats
									.getNumReceivedPackets()));
							activeRPCCount.setText(String.valueOf(stats
									.getNumRpcCalls()));

							RPCStats rpc = stats.getRpcStats();

							receivedBytesTotal.setText(formatters
									.formatByteCountToKiBEtc(rpc
											.getReceivedBytes()));
							sentBytesTotal
									.setText(formatters
											.formatByteCountToKiBEtc(rpc
													.getSentBytes()));
							receivedBytes.setText(formatters
									.formatByteCountToKiBEtcPerSec(rpc
											.getReceivedBytesPerSec()));
							sentBytes.setText(formatters
									.formatByteCountToKiBEtcPerSec(rpc
											.getSentBytesPerSec()));

							long uptimeSec = (System.currentTimeMillis() - stats
									.getStartedTimestamp()) / 1000;
							if (uptimeSec == 0) {
								uptimeSec = 1;
							}
							uptime.setText(formatters
									.formatTimeFromSeconds(uptimeSec));
							avgReceivedBytes.setText(formatters
									.formatByteCountToKiBEtcPerSec(rpc
											.getReceivedBytes()
											/ uptimeSec));
							avgSentBytes.setText(formatters
									.formatByteCountToKiBEtcPerSec(rpc
											.getSentBytes()
											/ uptimeSec));

							for (int i = 0; i < 4; i++) {
								Method m = Method.values()[i];
								Label[] messages = messageLabels[i];
								messages[0].setText(String.valueOf(rpc
										.getSentMessageCount(m, Type.REQ_MSG)));
								messages[1].setText(String.valueOf(rpc
										.getSentMessageCount(m, Type.RSP_MSG)));
								messages[2].setText(String.valueOf(rpc
										.getTimeoutMessageCount(m)));
								messages[3].setText(String.valueOf(rpc
										.getReceivedMessageCount(m,
												Type.REQ_MSG)));
								messages[4].setText(String.valueOf(rpc
										.getReceivedMessageCount(m,
												Type.RSP_MSG)));
							}
							dhtStatsGroup.layout();
							serverStatsGroup.layout();
							messageStatsGroup.layout();

							rtc.fullRepaint();

							TaskManager tman = plugin.getDHT(DHTView.this.type).getTaskManager();
							Task[] active = tman.getActiveTasks();
							Task[] queued = tman.getQueuedTasks();
							int size = active.length + queued.length;
							if (tasks == null || tasks.length != size) {
								tasks = new Task[size];
							}
							System
									.arraycopy(active, 0, tasks, 0,
											active.length);
							System.arraycopy(queued, 0, tasks, active.length,
									queued.length);
							taskTable.clearAll();
							taskTable.setItemCount(size);
						}
					});
				}
			}
		};
	}

	private void initialize (Composite comp) {
		//comp.setLayout(new GridLayout(1,false));
		GridData gridData = new GridData(GridData.FILL_BOTH);
		comp.setLayoutData(gridData);

		final ScrolledComposite scrollComposite = new ScrolledComposite(comp,
				SWT.V_SCROLL | SWT.H_SCROLL);

		final Composite comp_on_sc = new Composite(scrollComposite, SWT.None);

		GridLayout gl = new GridLayout(2, false);
		comp_on_sc.setLayout(gl);

		gridData = new GridData(GridData.FILL_BOTH);
		comp_on_sc.setLayoutData(gridData);

		createDHTStatsGroup(comp_on_sc);
		createControlGroup(comp_on_sc);
		createRPCGroup(comp_on_sc);
		createMessageStatsGroup(comp_on_sc);

		createRoutingTableView(comp_on_sc);
		createTaskTable(comp_on_sc);

		scrollComposite.setContent(comp_on_sc);
		scrollComposite.setExpandVertical(true);
		scrollComposite.setExpandHorizontal(true);
		scrollComposite.addControlListener(new ControlAdapter() {
			@Override
			public void controlResized (ControlEvent e) {
				scrollComposite.setMinSize(comp_on_sc.computeSize(SWT.DEFAULT,
						SWT.DEFAULT));
			}
		});
	}

	//----------------------------------------------------------------

	/**
	 * @param comp
	 */
	private void createControlGroup (Composite comp) {
		Group grp = new Group(comp, SWT.None);
		grp.setText("DHT Control");

		GridLayout gl = new GridLayout(3, false);
		grp.setLayout(gl);

		GridData gd = new GridData(GridData.FILL_HORIZONTAL);
		grp.setLayoutData(gd);

		Label ourIDLabel = new Label(grp, SWT.None);
		ourIDLabel.setText("Our ID:");

		ourID = new Label(grp, SWT.None);
		ourID.setText("XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX");

		gd = new GridData();
		gd.horizontalSpan = 2;
		ourID.setLayoutData(gd);

		Label dhtStatusLabel = new Label(grp, SWT.None);
		dhtStatusLabel.setText("DHT Status:");

		dhtRunStatus = new Label(grp, SWT.None);

		Button donation = new Button(grp, SWT.FLAT);
		gd = new GridData();
		gd.verticalSpan = 3;
		gd.horizontalAlignment = SWT.CENTER;
		gd.verticalAlignment = SWT.CENTER;
		donation.setLayoutData(gd);
		donationImg = new Image(
				grp.getDisplay(),
				DHTView.class
						.getResourceAsStream("/lbms/plugins/mldht/azureus/gui/paypal.gif"));
		donation.setImage(donationImg);
		donation
				.setToolTipText("Developing this plugin takes much time, if you want to help. Click here.");
		donation.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected (SelectionEvent e) {
				Program.launch(DONATION_URL);
			}
		});

		dhtStartStop = new Button(grp, SWT.PUSH);

		dhtStartStop.addSelectionListener(new SelectionAdapter() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
			 */
			@Override
			public void widgetSelected (SelectionEvent e) {
				if (plugin.getDHT(type).isRunning()) {
					plugin.stopDHT();
					deactivate();
				} else {
					plugin.startDHT();
					activate();
				}
				updateDHTRunStatus();
			}
		});

		Button bootstrap = new Button(grp, SWT.PUSH);
		bootstrap.setText("Bootstrap");
		bootstrap.addSelectionListener(new SelectionAdapter() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
			 */
			@Override
			public void widgetSelected (SelectionEvent e) {
				plugin.getDHT(type).bootstrap();
			}
		});

		Label logLevelLabel = new Label(grp, SWT.None);
		logLevelLabel.setText("DHT LogLevel:");

		final Combo logComb = new Combo(grp, SWT.DROP_DOWN | SWT.READ_ONLY);

		LogLevel[] levels = LogLevel.values();
		for (int i = 0; i < levels.length; i++) {
			logComb.add(levels[i].toString());
		}

		logComb.select(DHT.getLogLevel().ordinal());

		logComb.addSelectionListener(new SelectionAdapter() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
			 */
			@Override
			public void widgetSelected (SelectionEvent e) {
				DHT.setLogLevel(LogLevel.values()[logComb.getSelectionIndex()]);
			}
		});

		updateDHTRunStatus();
	}

	private void createDHTStatsGroup (Composite comp) {
		dhtStatsGroup = new Group(comp, SWT.None);
		Group grp = dhtStatsGroup;
		grp.setText("DHT Stats");

		GridLayout gl = new GridLayout(2, true);
		grp.setLayout(gl);

		GridData gd = new GridData(GridData.FILL_HORIZONTAL);
		grp.setLayoutData(gd);

		Label peerLabel = new Label(grp, SWT.None);
		peerLabel.setText("Peers in routing table:");

		peerCount = new Label(grp, SWT.None);
		peerCount.setText("0");

		Label taskLabel = new Label(grp, SWT.None);
		taskLabel.setText("Active Task:");

		taskCount = new Label(grp, SWT.None);
		taskCount.setText("0");

		Label dbKeysLabel = new Label(grp, SWT.None);
		dbKeysLabel.setText("Stored Keys:");

		keysCount = new Label(grp, SWT.None);
		keysCount.setText("0");

		Label dbItemsLabel = new Label(grp, SWT.None);
		dbItemsLabel.setText("Stored Items:");

		itemsCount = new Label(grp, SWT.None);
		itemsCount.setText("0");

		Label sentPacketsLabel = new Label(grp, SWT.None);
		sentPacketsLabel.setText("Sent Packets:");

		sentPacketCount = new Label(grp, SWT.None);
		sentPacketCount.setText("0");

		Label receivedPacketsLabel = new Label(grp, SWT.None);
		receivedPacketsLabel.setText("Received Packets:");

		receivedPacketCount = new Label(grp, SWT.None);
		receivedPacketCount.setText("0");

		Label rpcCallsLabel = new Label(grp, SWT.None);
		rpcCallsLabel.setText("Active Calls:");

		activeRPCCount = new Label(grp, SWT.None);
		activeRPCCount.setText("0");
	}

	private void createRPCGroup (Composite comp) {
		serverStatsGroup = new Group(comp, SWT.None);
		Group grp = serverStatsGroup;
		grp.setText("Server Stats");

		GridLayout gl = new GridLayout(2, true);
		grp.setLayout(gl);

		GridData gd = new GridData(GridData.FILL_HORIZONTAL);
		grp.setLayoutData(gd);

		Label rBytesLabel = new Label(grp, SWT.None);
		rBytesLabel.setText("Received Total:");

		receivedBytesTotal = new Label(grp, SWT.None);

		Label sBytesLabel = new Label(grp, SWT.None);
		sBytesLabel.setText("Sent Total:");

		sentBytesTotal = new Label(grp, SWT.None);

		Label rBytesPSLabel = new Label(grp, SWT.None);
		rBytesPSLabel.setText("Received:");

		receivedBytes = new Label(grp, SWT.None);

		Label sBytesPSLabel = new Label(grp, SWT.None);
		sBytesPSLabel.setText("Sent:");

		sentBytes = new Label(grp, SWT.None);

		Label runningSinceLabel = new Label(grp, SWT.None);
		runningSinceLabel.setText("Uptime:");

		uptime = new Label(grp, SWT.None);

		Label avgRecLabel = new Label(grp, SWT.None);
		avgRecLabel.setText("Avg. Received:");

		avgReceivedBytes = new Label(grp, SWT.None);

		Label avgSentLabel = new Label(grp, SWT.None);
		avgSentLabel.setText("Avg. Sent:");

		avgSentBytes = new Label(grp, SWT.None);
	}

	private void createMessageStatsGroup (Composite comp) {
		messageStatsGroup = new Group(comp, SWT.None);
		Group grp = messageStatsGroup;
		grp.setText("Message Stats");

		messageLabels = new Label[4][5];

		GridLayout gl = new GridLayout(6, false);
		grp.setLayout(gl);

		GridData gd = new GridData(GridData.FILL_HORIZONTAL);
		grp.setLayoutData(gd);

		//empty label
		new Label(grp, SWT.None);

		Label sentLabel = new Label(grp, SWT.None);
		sentLabel.setText("Sent");
		gd = new GridData();
		gd.horizontalSpan = 3;
		sentLabel.setLayoutData(gd);

		Label receivedLabel = new Label(grp, SWT.None);
		receivedLabel.setText("Received");
		gd = new GridData();
		gd.horizontalSpan = 2;
		receivedLabel.setLayoutData(gd);

		//empty label
		new Label(grp, SWT.None);

		Label sentRequestLabel = new Label(grp, SWT.None);
		sentRequestLabel.setText("Requests");
		Label sentResponseLabel = new Label(grp, SWT.None);
		sentResponseLabel.setText("Responses");
		Label sentTimeoutLabel = new Label(grp, SWT.None);
		sentTimeoutLabel.setText("Timeouts");

		Label recRequestLabel = new Label(grp, SWT.None);
		recRequestLabel.setText("Requests");
		Label recResponseLabel = new Label(grp, SWT.None);
		recResponseLabel.setText("Responses");

		Label pingLabel = new Label(grp, SWT.None);
		pingLabel.setText("Ping:");
		for (int i = 0; i < messageLabels[Method.PING.ordinal()].length; i++) {
			messageLabels[Method.PING.ordinal()][i] = new Label(grp, SWT.None);
		}

		Label findNodeLabel = new Label(grp, SWT.None);
		findNodeLabel.setText("Find Node:");
		for (int i = 0; i < messageLabels[Method.PING.ordinal()].length; i++) {
			messageLabels[Method.FIND_NODE.ordinal()][i] = new Label(grp,
					SWT.None);
		}

		Label getPeersLabel = new Label(grp, SWT.None);
		getPeersLabel.setText("Get Peers:");
		for (int i = 0; i < messageLabels[Method.PING.ordinal()].length; i++) {
			messageLabels[Method.GET_PEERS.ordinal()][i] = new Label(grp,
					SWT.None);
		}

		Label announceLabel = new Label(grp, SWT.None);
		announceLabel.setText("Announce");
		for (int i = 0; i < messageLabels[Method.PING.ordinal()].length; i++) {
			messageLabels[Method.ANNOUNCE_PEER.ordinal()][i] = new Label(grp,
					SWT.None);
		}
	}

	private void createRoutingTableView (Composite comp) {
		/*
		 * ScrolledComposite sc = new ScrolledComposite(comp, SWT.H_SCROLL |
		 * SWT.V_SCROLL | SWT.BORDER);
		 */

		Composite sc = new Composite(comp, SWT.None);

		GridData gd = new GridData();
		gd.horizontalSpan = 2;

		sc.setLayoutData(gd);

		rtc = new RoutingTableCanvas(sc);
	}

	private void createTaskTable (Composite comp) {
		taskTable = new Table(comp, SWT.VIRTUAL | SWT.BORDER
				| SWT.FULL_SELECTION);

		GridData gd = new GridData(GridData.FILL_BOTH);
		gd.horizontalSpan = 2;
		gd.minimumWidth = 150;

		taskTable.setLayoutData(gd);

		TableColumn taskTypeCol = new TableColumn(taskTable, SWT.None);
		taskTypeCol.setText("Type");
		taskTypeCol.setWidth(80);

		TableColumn statusCol = new TableColumn(taskTable, SWT.None);
		statusCol.setText("Status");
		statusCol.setWidth(50);

		TableColumn keyCol = new TableColumn(taskTable, SWT.None);
		keyCol.setText("Key");
		keyCol.setWidth(280);

		TableColumn reqCol = new TableColumn(taskTable, SWT.None);
		reqCol.setText("Active Requests");
		reqCol.setWidth(90);
		reqCol.setToolTipText("Requests: Active (Active+Stalled)");

		TableColumn msgCol = new TableColumn(taskTable, SWT.None);
		msgCol.setText("Messages");
		msgCol.setWidth(90);
		msgCol.setToolTipText("Messages: Sent | Received/Failed ");

		
		TableColumn infoCol = new TableColumn(taskTable, SWT.None);
		infoCol.setText("Info");
		infoCol.setWidth(700);

		taskTable.setHeaderVisible(true);

		taskTable.addListener(SWT.SetData, new Listener() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see org.eclipse.swt.widgets.Listener#handleEvent(org.eclipse.swt.widgets.Event)
			 */
			public void handleEvent (Event event) {
				TableItem item = (TableItem) event.item;
				int index = taskTable.indexOf(item);
				if (tasks != null && tasks.length > index) {
					Task t = tasks[index];
					if (t == null) {
						System.err.println("Task was null.");
						return;
					}
					if (t instanceof PeerLookupTask) {
						item.setText(0, ((PeerLookupTask) t).isNoAnnounce() ? "Scrape" : "Get Peers");
					} else if (t instanceof AnnounceTask) {
						item.setText(0, "Announce");
					} else if (t instanceof NodeLookup) {
						item.setText(0, "NodeLookup");
					} else if (t instanceof PingRefreshTask) {
						item.setText(0, "PingRefresh");
					} else {
						item.setText(0, t.getClass().getName());
					}

					if (t.isQueued()) {
						item.setText(1, "Queued");
					} else {
						item.setText(1, "Active");
					}

					if (t.getTargetKey() != null) {
						item.setText(2, t.getTargetKey().toString());
					} else {
						item.setText(2, "No Key");
					}

					item.setText(3, t.getNumOutstandingRequestsExcludingStalled() + " ("
							+ t.getNumOutstandingRequests() + ")");
					
					item.setText(4, t.getSentReqs() + " | " + t.getRecvResponses()
						+ "/" + t.getFailedReqs());

					if (t.getInfo() != null) {
						item.setText(5, t.getInfo());
					}
				}
			}
		});

		taskTable.pack();
	}

	//----------------------------------------------------------------

	private void updateDHTRunStatus () {
		isRunning = plugin.getDHT(type).isRunning();
		if (dhtRunStatus != null && !dhtRunStatus.isDisposed()) {
			dhtRunStatus.setText((plugin.getDHT(type).isRunning()) ? "Running"
					: "Stopped");
		}

		if (dhtStartStop != null && !dhtStartStop.isDisposed()) {
			dhtStartStop.setText((plugin.getDHT(type).isRunning()) ? "Stop"
					: "Start");
		}
		if (ourID != null && !ourID.isDisposed()) {
			ourID.setText((plugin.getDHT(type).isRunning()) ? plugin.getDHT(type)
					.getOurID().toString()
					: "XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX");
		}
	}

	//----------------------------------------------------------------

	private void delete () {
		deactivate();
		peerCount = null;
		taskCount = null;
		dhtRunStatus = null;
		dhtStartStop = null;
		rtc.dispose();
		if (donationImg != null) {
			donationImg.dispose();
			donationImg = null;
		}
		isCreated = false;
	}

	private void activate () {
		isRunning = plugin.getDHT(type).isRunning();
		if (!isCreated || !isRunning || isActivated) {
			return;
		}
		plugin.getDHT(type).addStatsListener(dhtStatsListener);
		if (plugin.getDHT(type).isRunning()) {
			rtc.setNode(plugin.getDHT(type).getNode());
		}
		updateDHTRunStatus();
		isActivated = true;
	}

	private void deactivate () {
		if (!isActivated) {
			return;
		}
		plugin.getDHT(type).removeStatsListener(dhtStatsListener);
		rtc.setNode(null);

		isActivated = false;
	}

	/*
	 * (non-Javadoc)
	 * 
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
