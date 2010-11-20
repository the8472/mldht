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
package lbms.plugins.mldht.indexer;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.LogLevel;

public class NIOConnectionManager {
	
	static NIOConnectionManager singleton = new NIOConnectionManager();
	
	static NIOConnectionManager getInstance() {
		return singleton;
	}
	
	ConcurrentLinkedQueue<Selectable> registrations = new ConcurrentLinkedQueue<Selectable>();
	List<Selectable> connections = new ArrayList<Selectable>(); 
	
	Selector selector;
	boolean running = true;
	
	public NIOConnectionManager() {
		try
		{
			selector = Selector.open();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		Thread t = new Thread(run);
		t.setName("mlDHT Metadata Connection Handler");
		t.setDaemon(true);
		t.start();
	}
	
	Runnable run = new Runnable() {
		public void run() {
			while(running)
			{
				try
				{
					if(selector.selectNow() == 0)
					{
						Thread.sleep(100);
					}
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
						conn.doTimeOutChecks(now);
					
					// register new connections
					Selectable toRegister = null;
					while((toRegister = registrations.poll()) != null)
					{
						connections.add(toRegister);
						toRegister.getChannel().register(selector, 0,toRegister);
						toRegister.registrationEvent();
					}
						
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
			}
		}
	}; 
	
	public void deRegister(Selectable connection)
	{
		connections.remove(connection);
	}
	
	public void register(Selectable connection) 
	{
		registrations.add(connection);
	}
	
	public void setSelection(Selectable connection, int mask, boolean onOff)
	{
		SelectionKey key = connection.getChannel().keyFor(selector);
		if(onOff)
			key.interestOps(key.interestOps() | mask);
		else
			key.interestOps(key.interestOps() & ~mask);
	}
}
