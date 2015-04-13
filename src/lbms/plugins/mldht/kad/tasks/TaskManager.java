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
package lbms.plugins.mldht.kad.tasks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCServer;

/**
 * Manages all dht tasks.
 *
 * @author Damokles
 */
public class TaskManager {

	private ConcurrentHashMap<Key, Deque<Task>> queued;
	private ConcurrentSkipListSet<Task>	tasks;
	private DHT					dht;
	private AtomicInteger		next_id = new AtomicInteger();
	private TaskListener		finishListener 	= new TaskListener() {
		public void finished(Task t) {
			tasks.remove(t);				
			dht.getStats().taskFinished(t);
			dequeue(t.getRPC().getDerivedID());
		}
	};

	public TaskManager (DHT dht) {
		this.dht = dht;
		tasks = new ConcurrentSkipListSet<Task>();
		queued = new ConcurrentHashMap<Key, Deque<Task>>();
		next_id.set(1);
	}
	
	public void addTask(Task task)
	{
		addTask(task, false);
	}
	
	// dequeue tasks for a specific server
	public void dequeue(Key k)
	{
		Task t = null;
		Deque<Task> q = queued.get(k);
		synchronized (q) {
			while ((t = q.peekFirst()) != null && canStartTask(t)) {
				q.removeFirst();
				tasks.add(t);
				t.start();
			}
		}
	}
	
	
	public void dequeue() {
		for(Key k : queued.keySet())
			dequeue(k);
	}

	/**
	 * Add a task to manage.
	 * @param task
	 */
	public void addTask (Task task, boolean isPriority) {
		int id = next_id.incrementAndGet();
		task.addListener(finishListener);
		task.setTaskID(id);
		if (!task.isQueued())
		{
			tasks.add(task);
			return;
		}
		
		Key rpcId = task.getRPC().getDerivedID();
		Deque<Task> q = queued.get(rpcId);
		if (q == null)
		{
			Deque<Task> t = new LinkedList<Task>();
			q = queued.putIfAbsent(rpcId, t);
			if(q == null)
				q = t;
		}
			
		synchronized (q)
		{
			if (isPriority)
				q.addFirst(task);
			else
				q.addLast(task);
		}
	}

	/// Get the number of running tasks
	public int getNumTasks () {
		return tasks.size();
	}

	/// Get the number of queued tasks
	public int getNumQueuedTasks () {
		return queued.size();
	}

	public Task[] getActiveTasks () {
		Task[] t = tasks.toArray(new Task[tasks.size()]);
		Arrays.sort(t);
		return t;
	}

	public Task[] getQueuedTasks () {
		List<Task> temp = new ArrayList<Task>();
		for(Deque<Task> q : queued.values())
			synchronized (q)
			{
				temp.addAll(q);				
			}
		return temp.toArray(new Task[temp.size()]);
	}
	
	public boolean canStartTask (Task toCheck) {
		// we can start a task if we have less then  7 runnning per server and
		// there are at least 16 RPC slots available
		return getNumTasks() < DHTConstants.MAX_ACTIVE_TASKS * Math.max(1, dht.getServerManager().getActiveServerCount()) && toCheck.getRPC().getNumActiveRPCCalls() + 16 < DHTConstants.MAX_ACTIVE_CALLS;
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("next id: ").append(next_id).append('\n');
		b.append("#### active: \n");
		
		for(Task t : tasks)
			b.append(t.toString());
		
		b.append("#### queued: \n");
		
		for(Task t : getQueuedTasks())
			b.append(t.toString());

		
		return b.toString();
	}

}
