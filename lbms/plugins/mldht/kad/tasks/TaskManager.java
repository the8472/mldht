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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import lbms.plugins.mldht.kad.DHT;

/**
 * Manages all dht tasks.
 *
 * @author Damokles
 */
public class TaskManager {

	private ConcurrentSkipListSet<Task>	tasks;
	private Deque<Task>			queued;
	private DHT					dht;
	private AtomicInteger		next_id = new AtomicInteger();
	private TaskListener		finishListener 	= new TaskListener() {
		public void finished(Task t) {
			tasks.remove(t);				
			dht.getStats().taskFinished(t);
			dequeue();
		}
	};

	public TaskManager (DHT dht) {
		this.dht = dht;
		tasks = new ConcurrentSkipListSet<Task>();
		queued = new LinkedList<Task>();
		next_id.set(1);
	}
	
	public void addTask(Task task)
	{
		addTask(task, false);
	}
	
	
	public void dequeue() {
		synchronized (queued) {
			Task t = null;
			while ((t = queued.peekFirst()) != null && dht.canStartTask(t)) {
				queued.removeFirst();
				tasks.add(t);
				t.start();
			}
		}
	}

	/**
	 * Add a task to manage.
	 * @param task
	 */
	public void addTask (Task task, boolean isPriority) {
		int id = next_id.incrementAndGet();
		task.addListener(finishListener);
		task.setTaskID(id);
		if (task.isQueued()) {
			synchronized (queued) {
				if(isPriority)
					queued.addFirst(task);
				else
					queued.addLast(task);
			}
		} else {
				tasks.add(task);
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
		synchronized (queued) {
			return queued.toArray(new Task[queued.size()]);
		}
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("next id: ").append(next_id).append('\n');
		b.append("#### active: \n");
		
		for(Task t : tasks)
			b.append(t.toString());
		
		b.append("#### queued: \n");
		
		synchronized (queued)
		{
			for(Task t : queued)
				b.append(t.toString());
		}
		
		return b.toString();
	}

}
