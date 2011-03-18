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
package lbms.plugins.mldht.utlis;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import sun.net.www.content.audio.wav;

import lbms.plugins.mldht.indexer.Selectable;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.LogLevel;

public class NIOConnectionManager {
	
	ConcurrentLinkedQueue<Selectable> registrations = new ConcurrentLinkedQueue<Selectable>();
	List<Selectable> connections = new ArrayList<Selectable>();
	//Thread workerThread;
	AtomicReference<Thread> workerThread = new AtomicReference<Thread>();
	
	String name;
	Selector selector;
	volatile boolean isSelecting;
	
	public NIOConnectionManager(String name) {
		this.name = name;
		try
		{
			selector = Selector.open();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	Runnable run = new Runnable() {
		public void run() {
			
			int iterations = 0;
			
			while(true)
			{
				try
				{
					isSelecting = true;
					selector.select(100);
					isSelecting = false;
					
					// handle active connections
					Set<SelectionKey> keys = selector.selectedKeys();
					for(SelectionKey selKey : keys)
					{
						Selectable connection = (Selectable) selKey.attachment();
						connection.selectionEvent(selKey);
					}
					keys.clear();
					
					// check existing connections
					long now = System.currentTimeMillis();
					for(Selectable conn : new ArrayList<Selectable>(connections))
						conn.doStateChecks(now);
					
					// register new connections
					Selectable toRegister = null;
					while((toRegister = registrations.poll()) != null)
					{
						connections.add(toRegister);
						toRegister.getChannel().register(selector, 0,toRegister);
						toRegister.registrationEvent(NIOConnectionManager.this);
					}
						
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
				
				iterations++;
				
				if(connections.size() == 0 && registrations.peek() == null)
				{
					if(iterations > 10)
					{
						workerThread.set(null);
						ensureRunning();
						break;
					}
				} else
				{
					iterations = 0;
				}
					
			}
		}
	};
	
	public void wakeup() {
		selector.wakeup();
	}
	
	private void ensureRunning() {
		while(true)
		{
			Thread current = workerThread.get();
			if(current == null && registrations.peek() != null)
			{
				current = new Thread(run);
				current.setName(name);
				current.setDaemon(true);
				if(workerThread.compareAndSet(null, current))
				{
					current.start();
					break;
				}
			} else
			{
				break;
			}
		}
	}
	
	public void deRegister(Selectable connection)
	{
		connections.remove(connection);
	}
	
	public void register(Selectable connection) 
	{
		registrations.add(connection);
		ensureRunning();
		wakeup();
	}
	
	public void asyncSetSelection(Selectable connection, int mask)
	{
		connection.getChannel().keyFor(selector).interestOps(mask);
		if(isSelecting)
			selector.wakeup();
	}

	/**
	 * note: this method is not thread-safe 
	 */
	public void setSelection(Selectable connection, int mask, boolean onOff)
	{
		SelectionKey key = connection.getChannel().keyFor(selector);
		if(onOff)
			key.interestOps(key.interestOps() | mask);
		else
			key.interestOps(key.interestOps() & ~mask);
		//wakeup();
	}
}
