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

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;

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
	
	
	
	private static int BUCKET_SPACING = 4;
	private static int BUCKET_PADDING = 2;
	private static int BUCKET_BORDER_LINE_WIDTH = 1;
	private int bucketContentWidth = 0;
	private static int BUCKET_DEPTH_OFFSET = 5;
	private int bucketXOffset = 0;
	

	private Canvas					canvas;
	private Image					img;
	private Display					display;
	private Shell					toolTipShell;
	private boolean					disposed;
	private int						peerWidth;
	private int						lineSpacing;
	private Node					routingTable;

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
		
		bucketContentWidth = DHTConstants.MAX_ENTRIES_PER_BUCKET * peerWidth + (DHTConstants.MAX_ENTRIES_PER_BUCKET -1) * X_SPACING; 
		bucketXOffset = bucketContentWidth;
		bucketXOffset += 2* BUCKET_PADDING;
		bucketXOffset += 2* BUCKET_BORDER_LINE_WIDTH;
		bucketXOffset += BUCKET_SPACING;

	}

	private void showTooltip (Event event) {
		if (toolTipShell != null && !toolTipShell.isDisposed()) {
			toolTipShell.dispose();
		}

		if (routingTable == null || disposed) {
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
		
		int nthBucket = x / bucketXOffset;
		RoutingTableEntry e = routingTable.getBuckets().get(nthBucket);
		int yOffset = e.prefix.getDepth() * BUCKET_DEPTH_OFFSET + BUCKET_PADDING;
		int xOffset = bucketXOffset * nthBucket + BUCKET_PADDING;
		
		
		if(y < yOffset)
			return null;
		
		boolean isMainBucket = true;
		boolean isReplacementBucket = true;
		
		isMainBucket &= yOffset < y && y < yOffset + PEER_HEIGHT;
		yOffset += PEER_HEIGHT + Y_SPACING;
		isReplacementBucket &= yOffset < y && y < yOffset + PEER_HEIGHT;
		
		int peerNum = (x - xOffset) / (peerWidth + X_SPACING);
		
		if(!isMainBucket && !isReplacementBucket || (x-xOffset-peerNum*(peerWidth+X_SPACING)) >= peerWidth)
			return null;
		
		List<KBucketEntry> bucket = isMainBucket? e.getBucket().getEntries() : e.getBucket().getReplacementEntries();
		if(peerNum >= bucket.size())
			return null;
		
		KBucketEntry bucketEntry = bucket.get(peerNum);
		
		String ver = bucketEntry.getVersion();
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
		

		String tip = "ID: " + bucketEntry.getID().toString();
		tip += "\nAddress: " + bucketEntry.getAddress();
		tip += "\nClientVer: " + ver;
		tip += "\nLast Responded: "
				+ ((System.currentTimeMillis() - bucketEntry.getLastSeen()) / 1000)
				+ "sec";
		tip += "\nAge: " + (System.currentTimeMillis() - bucketEntry.getCreationTime())
				/ 1000 + "sec";
		tip += "\nFailed Queries: " + bucketEntry.getFailedQueries();
		return tip;
	}

	public void fullRepaint () {
		if (disposed) {
			return;
		}
		GC gc = new GC(img);
		printBackground(gc);
		if (routingTable != null) {
			printPeers(gc);
		}
		canvas.redraw();
		gc.dispose();
	}

	private void printBackground (GC gc) {
		gc.setBackground(display.getSystemColor(SWT.COLOR_WHITE));
		gc.fillRectangle(img.getBounds());
	}

	private void printBuckets (GC gc) {
		/*
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
		*/
	}

	private void printPeers (GC gc) {

		
		List<RoutingTableEntry> buckets = routingTable.getBuckets(); 
		
		
		for (int i = 0; i < buckets.size(); i++) {
			RoutingTableEntry rtEntry = buckets.get(i);
			
			int currentBucketOffsetX = i * bucketXOffset;
			int currentBucketOffsetY = rtEntry.prefix.getDepth() * BUCKET_DEPTH_OFFSET; 
			
			gc.setAlpha(255);
			gc.drawRectangle(currentBucketOffsetX, currentBucketOffsetY , bucketContentWidth + 2* BUCKET_PADDING, 3 * PEER_HEIGHT);
			
			currentBucketOffsetX += BUCKET_PADDING;
			currentBucketOffsetY += BUCKET_PADDING;
			
			List<KBucketEntry> entries = rtEntry.getBucket().getEntries();
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
				int xOffset = currentBucketOffsetX + j * (peerWidth + X_SPACING);

				gc.fillRectangle(xOffset, currentBucketOffsetY, peerWidth, PEER_HEIGHT);
			}
			
			currentBucketOffsetY += PEER_HEIGHT + Y_SPACING;

			gc.setBackground(display.getSystemColor(SWT.COLOR_GRAY));
			gc.fillRectangle(currentBucketOffsetX, currentBucketOffsetY, bucketContentWidth, PEER_HEIGHT);
			gc.drawRectangle(currentBucketOffsetX, currentBucketOffsetY, bucketContentWidth, PEER_HEIGHT);
			
			gc.setAlpha(128);
			
			entries = rtEntry.getBucket().getReplacementEntries();
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
				int xOffset = currentBucketOffsetX + j * (peerWidth + X_SPACING);

				gc.fillRectangle(xOffset, currentBucketOffsetY, peerWidth, PEER_HEIGHT);
			}
		}
	}

	public void setNode (Node bucketHolder) {
		routingTable = bucketHolder;
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
