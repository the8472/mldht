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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
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

	private ConcurrentHashMap<Key, NavigableSet<Task>> queued;
	private ConcurrentSkipListSet<Task>	tasks;
	private DHT					dht;
	private AtomicInteger		next_id = new AtomicInteger();
	private TaskListener		finishListener 	= t -> {
		dht.getStats().taskFinished(t);
		dequeue(t.getRPC().getDerivedID());
		tasks.remove(t);
	};
	
	static final Comparator<Task> comp;
	
	static {
		
		Comparator<Task> prio = Comparator.comparing(Task::isMaintenanceTask).reversed();
		Comparator<Task> group = Comparator.comparingInt(t -> t.getTaskID() >>> 5);
		Comparator<Task> targeted = Comparator.comparing(t -> (t instanceof TargetedTask) ? ((TargetedTask)t).getTargetKey() : null, Comparator.nullsFirst(Key.BYTE_REVERSED_ORDER));
		
		// priority tasks first, then batches of 32 (derived from task it), then by LSBs of target id, then task id
		// this should mitigate task submissions that contain runs of shared prefixes
		comp = prio.thenComparing(group).thenComparing(targeted).thenComparing(Task::getTaskID);
		
	}

	public TaskManager (DHT dht) {
		this.dht = dht;
		tasks = new ConcurrentSkipListSet<>();
		queued = new ConcurrentHashMap<>();
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
		NavigableSet<Task> q = queued.get(k);
		synchronized (q) {
			while (!q.isEmpty() && (t = q.first()) != null && canStartTask(t)) {
				q.pollFirst();
				if(t.isFinished())
					continue;
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
		task.setMaintenanceTask(isPriority);
		int id = next_id.incrementAndGet();
		task.addListener(finishListener);
		task.setTaskID(id);
		if (!task.isQueued())
		{
			tasks.add(task);
			return;
		}
		
		Key rpcId = task.getRPC().getDerivedID();
		
		
		
		NavigableSet<Task> q = queued.computeIfAbsent(rpcId, k -> new TreeSet<>(comp));
			
		synchronized (q)
		{
			q.add(task);
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
		List<Task> temp = new ArrayList<>();
		for(NavigableSet<Task> q : queued.values())
			synchronized (q)
			{
				temp.addAll(q);
			}
		return temp.toArray(new Task[temp.size()]);
	}
	
	public boolean canStartTask (Task toCheck) {
		// we can start a task if we have less then  7 runnning per server and
		// there are at least 16 RPC slots available
		RPCServer srv = toCheck.getRPC();
		int perServer = (int) tasks.stream().filter(t -> t.getRPC().equals(srv)).count();
		return perServer < DHTConstants.MAX_ACTIVE_TASKS && srv.getNumActiveRPCCalls() + 16 < DHTConstants.MAX_ACTIVE_CALLS;
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("next id: ").append(next_id).append('\n');
		b.append("#### active: \n");
		
		for(Task t : tasks)
			b.append(t.toString()).append('\n');
		
		b.append("#### queued: \n");
		
		for(Task t : getQueuedTasks())
			b.append(t.toString()).append('\n');

		
		return b.toString();
	}

}
