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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.tasks.Task.TaskState;

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
	private TaskListener		finishListener 	= t -> {
		dht.getStats().taskFinished(t);
		tasks.remove(t);
		dequeue(t.getRPC().getDerivedID());
	};

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
		Deque<Task> q = queued.get(k);
		synchronized (q) {
			while ((t = q.peekFirst()) != null && canStartTask(t)) {
				q.removeFirst();
				if(t.isFinished())
					continue;
				tasks.add(t);
				dht.getScheduler().execute(t::start);
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
		if (task.state.get() == TaskState.RUNNING)
		{
			tasks.add(task);
			return;
		}
		
		if(!task.setState(TaskState.INITIAL, TaskState.QUEUED))
			return;
		
		
		
		Key rpcId = task.getRPC().getDerivedID();
		
		Deque<Task> q = queued.computeIfAbsent(rpcId, k -> new ArrayDeque<>());;
			
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
		List<Task> temp = new ArrayList<>();
		for(Deque<Task> q : queued.values())
			synchronized (q)
			{
				temp.addAll(q);
			}
		return temp.toArray(new Task[temp.size()]);
	}
	
	public boolean canStartTask (Task toCheck) {
		RPCServer srv = toCheck.getRPC();
		
		return canStartTask(srv);

	}
	
	public boolean canStartTask(RPCServer srv) {
		// we can start a task if we have less then  7 runnning per server and
		// there are at least 16 RPC slots available

		int activeCalls = srv.getNumActiveRPCCalls();
		if(activeCalls + 16 >= DHTConstants.MAX_ACTIVE_CALLS)
			return false;
		
		List<Task> currentServerTasks = tasks.stream().filter(t -> t.getRPC().equals(srv)).collect(Collectors.toList());
		int perServer = currentServerTasks.size();
		
		if(perServer < DHTConstants.MAX_ACTIVE_TASKS)
			return true;
		
		// if all their tasks have sent at least their initial volley and we still have enough head room we can allow more tasks.
		return activeCalls < (DHTConstants.MAX_ACTIVE_CALLS * 2) / 3 && currentServerTasks.stream().allMatch(t -> t.requestConcurrency() < t.getSentReqs());
	}
	
	public int queuedCount(RPCServer srv) {
		Deque<Task> q = queued.get(srv.getDerivedID());
		synchronized (q) {
			return q.size();
		}
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
