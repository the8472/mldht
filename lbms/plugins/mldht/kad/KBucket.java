package lbms.plugins.mldht.kad;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.tasks.Task;

/**
 * A KBucket is just a list of KBucketEntry objects.
 *
 * The list is sorted by time last seen :
 * The first element is the least recently seen, the last
 * the most recently seen.
 *
 * @author Damokles
 */
public class KBucket implements Externalizable {

	private static final long	serialVersionUID	= -5507455162198975209L;
	
	// any modifying actions to entries must happen through removeAndInsert (which is synchronized)!
	private LinkedList<KBucketEntry>	entries;
	// this is synchronized on entries, not on replacementBucket!
	private LinkedList<KBucketEntry>	replacementBucket;
	
	private Node						node;
	private Set<KBucketEntry>			pendingPings;
	private long						last_modified;
	private Task						refresh_task;
	
	public KBucket () {
		entries = new LinkedList<KBucketEntry>();
		replacementBucket = new LinkedList<KBucketEntry>();
		pendingPings = Collections.synchronizedSet(new HashSet<KBucketEntry>());	
	}

	public KBucket (Node node) {
		this();
		//last_modified = System.currentTimeMillis();
		this.node = node;
	}

	/**
	 * Inserts an entry into the bucket.
	 * @param entry The entry to insert
	 */
	public void insert (KBucketEntry entry) {
		removeAndInsert(null, entry);
	}
	
	private void removeAndInsert(KBucketEntry toRemove, KBucketEntry toInsert)
	{
		KBucketEntry toPing = null;

		insertTests: synchronized (entries)
		{
			if (toRemove != null) {
				entries.remove(toRemove);
			}
			if (toInsert == null) {
				return;
			}
			int idx = entries.indexOf(toInsert);
			if (idx != -1)
			{
				// If in the list, move it to the end
				final KBucketEntry oldEntry = entries.get(idx);
				final KBucketEntry newEntry = toInsert;
				if (!oldEntry.getAddress().equals(toInsert.getAddress()))
				{
					// node changed address, check if old node is really dead to prevent impersonation
					pingEntry(oldEntry, new RPCCallListener() {
						 
						public void onTimeout(RPCCallBase c) {
							removeAndInsert(oldEntry, newEntry);
							DHT.log("Node "+oldEntry.getID()+" changed address from "+oldEntry.getAddress()+" to "+newEntry.getAddress(), LogLevel.Info);
						}
						
						public void onStall(RPCCallBase c) {}
						
						public void onResponse(RPCCallBase c, MessageBase rsp) {
							DHT.log("New node "+newEntry.getAddress()+" claims same Node ID ("+oldEntry.getID()+") as "+oldEntry.getAddress()+" ; node dropped as this might be an impersonation attack", LogLevel.Error);
						}
					});
					break insertTests;
				}
				
				// only refresh the last seen time if it already exists
				oldEntry.mergeTimestamps(newEntry);
				adjustTimerOnInsert(oldEntry);
				break insertTests;
			}
			
			
			if (entries.size() < DHTConstants.MAX_ENTRIES_PER_BUCKET)
			{
				// insert if not already in the list and we still have room
				sortedInsert(toInsert);
				break insertTests;
			}
			
			if (replaceBadEntry(toInsert))
				break insertTests;

			// older entries displace younger ones (this code path is mostly used on changing node IDs)
			KBucketEntry youngest = entries.getLast();
			
			if (youngest.getCreationTime() > toInsert.getCreationTime())
			{
				entries.remove(youngest);
				sortedInsert(toInsert);
				toPing = youngest;
				break insertTests;
			}
			
			toPing = toInsert;
			

		}
		// ping outside the synchronized block to avoid deadlocks due to dual lock usage with entries + pending entries
		if(toPing != null) {
			pingQuestionable(toPing);
		}
	}
	
	/**
	 * mostly meant for internal use or transfering entries into a new bucket.
	 * to update a bucket properly use {@link #insert(KBucketEntry)} 
	 */
	public void sortedInsert(KBucketEntry toInsert) {
		if(toInsert == null)
			return;
		synchronized (entries)
		{
			KBucketEntry youngest = entries.peekLast();
			entries.addLast(toInsert);
			adjustTimerOnInsert(toInsert);
			if (youngest != null && toInsert.getCreationTime() < youngest.getCreationTime())
				Collections.sort(entries, KBucketEntry.AGE_ORDER);
			while(entries.size() > DHTConstants.MAX_ENTRIES_PER_BUCKET)
				insertInReplacementBucket(entries.removeLast());
				
		}
	}

	/**
	 * Get the number of entries.
	 *
	 * @return The number of entries in this Bucket
	 */
	public int getNumEntries () {
		return entries.size();
	}

	/**
	 * @return the entries
	 */
	public List<KBucketEntry> getEntries () {
		return new ArrayList<KBucketEntry>(entries);
	}
	
	public List<KBucketEntry> getReplacementEntries() {
		return new ArrayList<KBucketEntry>(replacementBucket);
	}

	/**
	 * Checks if this bucket contains an entry.
	 *
	 * @param entry Entry to check for
	 * @return true if found
	 */
	public boolean contains (KBucketEntry entry) {
		return entries.contains(entry);
	}

	/**
	 * A peer failed to respond
	 * @param addr Address of the peer
	 */
	public boolean onTimeout (InetSocketAddress addr) {
		synchronized (entries)
		{
			for (int i = 0; i < entries.size(); i++)
			{
				KBucketEntry e = entries.get(i);
				if (e.getAddress() == addr)
				{
					e.signalRequestTimeout();
					//only removes the entry if it is bad
					removeEntry(e, false);
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Check if the bucket needs to be refreshed
	 *
	 * @return true if it needs to be refreshed
	 */
	public boolean needsToBeRefreshed () {
		long now = System.currentTimeMillis();

		return (now - last_modified > DHTConstants.BUCKET_REFRESH_INTERVAL
				&& (refresh_task == null || refresh_task.isFinished()) && entries.size() > 0);
	}

	/**
	 * Resets the last modified for this Bucket
	 */
	public void updateRefreshTimer () {
		last_modified = System.currentTimeMillis();
	}
	

	public String toString() {
		return "entries: "+entries+" replacements: "+replacementBucket;
	}

	private void adjustTimerOnInsert(KBucketEntry entry)
	{
		if(entry.getLastSeen() > last_modified) {
			last_modified = entry.getLastSeen();
		}
	}

	/**
	 * Pings an entry and notifies the listener of the result.
	 *
	 * @param entry entry to ping
	 * @return true if the ping was sent, false if there already is an outstanding ping for that entry
	 */
	private boolean pingEntry(final KBucketEntry entry, RPCCallListener listener)
	{
		// don't ping if there already is an outstanding ping
		if(pendingPings.contains(entry)) {
	        return false;
        }

		PingRequest p = new PingRequest();
		p.setDestination(entry.getAddress());
		RPCCall c = node.getDHT().getRandomServer().doCall(p);
		if (c != null) {
			pendingPings.add(entry);
			c.addListener(listener);
			c.addListener(new RPCCallListener() {
				public void onTimeout(RPCCallBase c) {
					pendingPings.remove(entry);
				}
				
				// performance doesn't matter much here, ignore stalls
				public void onStall(RPCCallBase c) {}
				
				public void onResponse(RPCCallBase c, MessageBase rsp) {
					pendingPings.remove(entry);
				}
			});
			return true;
		}
		return false;
	}
	

	private void pingQuestionable (final KBucketEntry replacement_entry) {
		if (pendingPings.size() >= 2) {
			insertInReplacementBucket(replacement_entry);
			return;
		}

		// we haven't found any bad ones so try the questionable ones
		synchronized (entries)
		{
			for (final KBucketEntry toTest : entries)
			{
				if (toTest.isQuestionable() && pingEntry(toTest, new RPCCallListener() {
					public void onTimeout(RPCCallBase c) {
						removeAndInsert(toTest, replacement_entry);
						// we could replace this one, try another one.
						KBucketEntry nextReplacementEntry;
						synchronized (entries)
						{
							nextReplacementEntry = replacementBucket.pollLast();
						}
						if (replacement_entry != null && !replaceBadEntry(nextReplacementEntry))
							pingQuestionable(nextReplacementEntry);
					}
					
					// performance doesn't matter much here. ignore stall
					public void onStall(RPCCallBase c) {}

					public void onResponse(RPCCallBase c, MessageBase rsp) {
						
						
						// it's alive, check another one
						if (!replaceBadEntry(replacement_entry)) {
							pingQuestionable(replacement_entry);
						}
					}
				}))
				{
					return;
				}
			}
		}

		//save the entry if all are good
		insertInReplacementBucket(replacement_entry);

	}

	/**
	 * Tries to instert entry by replacing a bad entry.
	 *
	 * @param entry Entry to insert
	 * @return true if replace was successful
	 */
	private boolean replaceBadEntry (KBucketEntry entry) {
		for (KBucketEntry e : entries) {
			if (e.isBad()) {
				// bad one get rid of it
				removeAndInsert(e, entry);
				return true;
			}
		}
		return false;
	}
	
	private void insertInReplacementBucket(KBucketEntry entry)
	{
		synchronized (entries) {
			//if it is already inserted remove it and add it to the end
			int idx = replacementBucket.indexOf(entry);
			if (idx != -1) {
				KBucketEntry newEntry = entry;
				entry = replacementBucket.remove(idx);
				entry.mergeTimestamps(newEntry);
			}
			
			replacementBucket.addLast(entry);
			
			while (replacementBucket.size() > DHTConstants.MAX_ENTRIES_PER_BUCKET)
				replacementBucket.removeFirst(); //remove the least recently seen one
		}
	}
	
	/**
	 * only removes one bad entry at a time to prevent sudden flushing of the routing table	
	 */
	public void checkBadEntries() {

		synchronized (entries)
		{
			KBucketEntry toRemove = null;
			KBucketEntry replacement;
			for (KBucketEntry e : entries)
			{
				if (!e.isBad())
					continue;
				toRemove = e;
				break;
			}
			
			if(toRemove == null)
				return;
			replacement = replacementBucket.pollLast();
			if(replacement == null)
				return;
			entries.remove(toRemove);
			sortedInsert(replacement);
		}
	
	}
	
	public boolean checkForIDChangeAndNotifyOfResponse(MessageBase msg)
	{
		synchronized (entries)
		{
			// check if node changed its ID
			for (KBucketEntry entry : entries)
			{
				// node ID change detected, reassign node to the appropriate bucket
				if (entry.getAddress().equals(msg.getOrigin()) && !entry.getID().equals(msg.getID()))
				{
					removeEntry(entry, true); //remove and replace from replacement bucket
					DHT.log("Node " + entry.getAddress() + " changed ID from " + entry.getID() + " to " + msg.getID(), LogLevel.Info);
					KBucketEntry newEntry = new KBucketEntry(entry.getAddress(), msg.getID(), entry.getCreationTime());
					newEntry.mergeTimestamps(entry);
					// insert into appropriate bucket for the new ID
					node.insertEntry(newEntry,false);
					return true;
				}
				
				// no node ID change detected, update last responded. insert will be invoked soon, thus we don't have to do the move-to-end stuff				
				if(msg.getType() == Type.RSP_MSG && entry.getID().equals(msg.getID()))
					entry.signalResponse();
			}
		}
		return false;
	}


	/**
	 * @param toRemove Entry to remove, if its bad
	 * @param force if true entry will be removed regardless of its state
	 */
	public void removeEntry(KBucketEntry toRemove, boolean force) {
		synchronized (entries)
		{
			if (entries.contains(toRemove) && (force || toRemove.isBad()))
			{
				KBucketEntry replacement = null;
				replacement = replacementBucket.pollLast();
				// only remove if we have a replacement or really need to
				if(replacement != null || force)
				{
					entries.remove(toRemove);
					sortedInsert(replacement);					
				}
			}
			
		}
	}

	/**
	 * @param node the node to set
	 */
	public void setNode (Node node) {
		this.node = node;
	}

	/**
	 * @param refresh_task the refresh_task to set
	 */
	public void setRefreshTask (Task refresh_task) {
		this.refresh_task = refresh_task;
	}
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		Map<String,Object> serialized = (Map<String, Object>) in.readObject();
		Object obj = serialized.get("mainBucket");
		if(obj instanceof Collection<?>)
			entries.addAll((Collection<KBucketEntry>)obj);
		obj = serialized.get("replacementBucket");
		if(obj instanceof Collection<?>)
			replacementBucket.addAll((Collection<KBucketEntry>)obj);
		obj = serialized.get("lastModifiedTime");
		if(obj instanceof Long)
			last_modified = (Long)obj;
		obj = serialized.get("prefix");
		
		entries.removeAll(Collections.singleton(null));
		replacementBucket.removeAll(Collections.singleton(null));
		Collections.sort(entries, KBucketEntry.AGE_ORDER);
		Collections.sort(replacementBucket,KBucketEntry.LAST_SEEN_ORDER);
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		Map<String,Object> serialized = new HashMap<String, Object>();
		// put entries as any type of collection, will convert them on deserialisation
		serialized.put("mainBucket", entries);
		serialized.put("replacementBucket", replacementBucket);
		serialized.put("lastModifiedTime", last_modified);

		synchronized (entries)
		{
			out.writeObject(serialized);
		}
	}
}
