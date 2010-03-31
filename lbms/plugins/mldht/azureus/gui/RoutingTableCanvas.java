package lbms.plugins.mldht.azureus.gui;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lbms.plugins.mldht.kad.KBucket;
import lbms.plugins.mldht.kad.KBucketEntry;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

/**
 * @author Damokles
 * 
 */
public class RoutingTableCanvas {

	private static final int	HEADER_HEIGHT			= 15;

	private static final int	HIGHLIGHT_EVERY_X_LINE	= 5;

	private static final int	PEER_HEIGHT				= 15;
	private static final int	X_SPACING				= 1;
	private static final int	Y_SPACING				= 2;

	private static final int	DEFAULT_WIDTH			= 1120;
	private static final int	DEFAULT_HEIGHT			= (PEER_HEIGHT + Y_SPACING)
																* 8
																+ HEADER_HEIGHT
																+ 5;

	private Canvas				canvas;
	private Image				img;
	private Display				display;
	private Shell				toolTipShell;

	private boolean				disposed;

	private int					peerWidth;
	private int					lineSpacing;
	private KBucket[]			bucketList;

	public RoutingTableCanvas(Composite parent) {
		this(parent, null);
	}

	public RoutingTableCanvas(Composite parent, Object layoutData) {
		display = parent.getDisplay();
		img = new Image(display, DEFAULT_WIDTH, DEFAULT_HEIGHT);

		if (parent instanceof ScrolledComposite) {
			final ScrolledComposite sc = (ScrolledComposite) parent;
			canvas = new Canvas(parent, SWT.DOUBLE_BUFFERED);
			sc.setContent(canvas);
			sc.setExpandHorizontal(true);
			sc.setExpandVertical(true);
			sc.setMinHeight(DEFAULT_HEIGHT);
			sc.setMinWidth(DEFAULT_WIDTH);
		} else {
			canvas = new Canvas(parent, SWT.DOUBLE_BUFFERED | SWT.BORDER);
		}
		canvas.setSize(DEFAULT_WIDTH, DEFAULT_HEIGHT);
		calculateMeasures(DEFAULT_WIDTH, DEFAULT_HEIGHT);
		canvas.addPaintListener(new PaintListener() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.eclipse.swt.events.PaintListener#paintControl(org.eclipse
			 * .swt.events.PaintEvent)
			 */
			public void paintControl (PaintEvent e) {
				e.gc.drawImage(img, 0, 0);
			}
		});
		if (layoutData != null) {
			if (layoutData instanceof GridData) {
				GridData gd = (GridData) layoutData;
				gd.minimumWidth = DEFAULT_WIDTH;
				gd.minimumHeight = DEFAULT_HEIGHT;
				gd.heightHint = DEFAULT_HEIGHT;
				gd.widthHint = DEFAULT_WIDTH;
			}
			canvas.setLayoutData(layoutData);
		}

		Listener toolTipListener = new Listener() {
			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.eclipse.swt.widgets.Listener#handleEvent(org.eclipse.swt.
			 * widgets.Event)
			 */
			public void handleEvent (Event event) {
				switch (event.type) {
				case SWT.Dispose:
				case SWT.MouseMove:
					if (toolTipShell != null) {
						if (!toolTipShell.isDisposed()) {
							toolTipShell.dispose();
						}
						toolTipShell = null;
					}
					break;

				case SWT.MouseHover:
					showTooltip(event);
				}

			}
		};

		canvas.addListener(SWT.Dispose, toolTipListener);
		canvas.addListener(SWT.MouseMove, toolTipListener);
		canvas.addListener(SWT.MouseHover, toolTipListener);

	}

	private void calculateMeasures (int x, int y) {
		lineSpacing = x / 160;
		peerWidth = lineSpacing - X_SPACING - X_SPACING - 1; //-1 to compensate the actual line width
	}

	private void showTooltip (Event event) {
		if (toolTipShell != null && !toolTipShell.isDisposed()) {
			toolTipShell.dispose();
		}

		if (bucketList == null || disposed) {
			return;
		}

		String toolTipString = getTooltipForPoint(event.x, event.y);

		if (toolTipString == null) {
			return;
		}
		toolTipShell = new Shell(canvas.getShell(), SWT.ON_TOP | SWT.NO_FOCUS
				| SWT.TOOL);
		toolTipShell.setBackground(display
				.getSystemColor(SWT.COLOR_INFO_BACKGROUND));
		FillLayout layout = new FillLayout();
		layout.marginWidth = 2;
		toolTipShell.setLayout(layout);
		Label label = new Label(toolTipShell, SWT.NONE);
		label.setForeground(display.getSystemColor(SWT.COLOR_INFO_FOREGROUND));
		label.setBackground(display.getSystemColor(SWT.COLOR_INFO_BACKGROUND));

		label.setText(toolTipString);
		Point size = toolTipShell.computeSize(SWT.DEFAULT, SWT.DEFAULT);
		Point relativeLocation = canvas.toDisplay(0, 0);
		int xPosition = event.x + relativeLocation.x;
		int yPosition = event.y + relativeLocation.y;
		Rectangle displayBounds = display.getBounds();
		if (xPosition > (displayBounds.width / 100) * 90) {
			xPosition -= size.x + 10;
		} else {
			xPosition += 15;
		}

		if (yPosition > (displayBounds.height / 100) * 90) {
			yPosition -= size.y + 10;
		}
		toolTipShell.setBounds(xPosition, yPosition, size.x, size.y);
		toolTipShell.setVisible(true);
	}

	private String getTooltipForPoint (int x, int y) {
		int i = (x / lineSpacing) + 1;
		int j = (y - HEADER_HEIGHT) / (PEER_HEIGHT + Y_SPACING);

		int xOffset = (i - 1) * lineSpacing + X_SPACING;
		int yOffset = j * (PEER_HEIGHT + Y_SPACING) + Y_SPACING + HEADER_HEIGHT;
		Rectangle rect = new Rectangle(xOffset, yOffset, xOffset + peerWidth,
				yOffset + PEER_HEIGHT);
		if (!rect.contains(x, y)) {
			return null;
		}
		KBucketEntry e = null;
		if (i < 160 && i >= 0 && bucketList[i] != null
				&& bucketList[i].getNumEntries() > j) {
			e = bucketList[i].getEntries().get(j);
		}
		if (e == null) {
			return null;
		}
		
		String ver = e.getVersion();
		if(ver == null || ver.length() < 4)
			ver = "";
		else
		{
			try
			{
				byte[] raw = ver.getBytes("ISO-8859-1");
				ver = ver.substring(0, 2) + " " + ( (raw[2] & 0xFF) << 8 | (raw[3] & 0xFF));
			} catch (UnsupportedEncodingException e1)
			{
				e1.printStackTrace();
			}				
		}
		

		String tip = "ID: " + e.getID().toString();
		tip += "\nAddress: " + e.getAddress();
		tip += "\nClientVer: " + ver;
		tip += "\nLast Responded: "
				+ ((System.currentTimeMillis() - e.getLastSeen()) / 1000)
				+ "sec";
		tip += "\nAge: " + (System.currentTimeMillis() - e.getCreationTime())
				/ 1000 + "sec";
		tip += "\nFailed Queries: " + e.getFailedQueries();
		return tip;
	}

	public void fullRepaint () {
		if (disposed) {
			return;
		}
		GC gc = new GC(img);
		printBackground(gc);
		printLines(gc);
		if (bucketList != null) {
			printPeers(gc, bucketList);
		}
		canvas.redraw();
		gc.dispose();
	}

	private void printBackground (GC gc) {
		gc.setBackground(display.getSystemColor(SWT.COLOR_WHITE));
		gc.fillRectangle(img.getBounds());
	}

	private void printLines (GC gc) {
		gc.setForeground(display.getSystemColor(SWT.COLOR_BLACK));
		gc.setBackground(display.getSystemColor(SWT.COLOR_GRAY));
		for (int i = 1; i < 160; i++) {
			if (i % HIGHLIGHT_EVERY_X_LINE == 0) {
				gc.drawText(String.valueOf(i), lineSpacing * (i - 1), 2);
				gc.fillRectangle(lineSpacing * (i - 1) + 1, HEADER_HEIGHT,
						lineSpacing - 1, (PEER_HEIGHT + Y_SPACING) * 8 + 1);
			}
			gc.drawLine(lineSpacing * i, HEADER_HEIGHT, lineSpacing * i,
					(PEER_HEIGHT + Y_SPACING) * 8 + HEADER_HEIGHT);
		}
	}

	private void printPeers (GC gc, KBucket[] list) {
		for (int i = 0; i < list.length; i++) {
			if (list[i] == null) {
				continue;
			}
			List<KBucketEntry> entries = list[i].getEntries();
			for (int j = 0; j < entries.size(); j++) {
				KBucketEntry e = entries.get(j);
				if (e.isGood()) {
					Color c = display.getSystemColor(SWT.COLOR_DARK_GREEN);
					//gc.setForeground(c);
					gc.setBackground(c);
				} else if (e.isBad()) {
					Color c = display.getSystemColor(SWT.COLOR_RED);
					//gc.setForeground(c);
					gc.setBackground(c);
				} else {
					Color c = display.getSystemColor(SWT.COLOR_BLUE);
					//gc.setForeground(c);
					gc.setBackground(c);
				}
				int xOffset = lineSpacing * (i - 1) + X_SPACING + 1;
				int yOffset = j * (PEER_HEIGHT + Y_SPACING) + Y_SPACING
						+ HEADER_HEIGHT;
				gc.fillRectangle(xOffset, yOffset, peerWidth, PEER_HEIGHT);
			}
		}
	}

	/**
	 * @param bucketList the bucketList to set
	 */
	public void setBucketList (KBucket[] bucketList) {
		this.bucketList = bucketList;
	}

	public synchronized void dispose () {
		if (!disposed) {
			if (toolTipShell != null) {
				if (!toolTipShell.isDisposed()) {
					toolTipShell.dispose();
				}
				toolTipShell = null;
			}
			canvas.dispose();
			img.dispose();
			disposed = true;
		}
	}
}
