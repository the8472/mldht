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
package lbms.plugins.mldht.kad;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Stream;

import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;

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
	
	/**
	 * use {@link #insertOrRefresh}, {@link #sortedInsert} or {@link #removeEntry} to handle this<br>
	 * using copy-on-write semantics for this list, referencing it is safe if you make local copy
	 */
	private volatile List<KBucketEntry>	entries;
	
	private AtomicInteger						currentReplacementPointer;
	private AtomicReferenceArray<KBucketEntry>	replacementBucket;
	
	private Node						node;
	private long						lastRefresh;
	
	public KBucket () {
		entries = new ArrayList<KBucketEntry>(); // using arraylist here since reading/iterating is far more common than writing.
		currentReplacementPointer = new AtomicInteger(0);
		replacementBucket = new AtomicReferenceArray<KBucketEntry>(DHTConstants.MAX_ENTRIES_PER_BUCKET);
	}

	public KBucket (Node node) {
		this();
		//last_modified = System.currentTimeMillis();
		this.node = node;
	}

	/**
	 * Notify bucket of new incoming packet from a node, perform update or insert existing nodes where appropriate
	 * @param newEntry The entry to insert
	 */
	public void insertOrRefresh (final KBucketEntry newEntry) {
		if (newEntry == null)
			return;
		
		List<KBucketEntry> entriesRef = entries;
		
		for(KBucketEntry existing : entriesRef) {
			if(existing.equals(newEntry) && newEntry.getAddress().getPort() == existing.getAddress().getPort()) {
				existing.mergeInTimestamps(newEntry);
				return;
			}
			
			if(existing.matchIPorID(newEntry)) {
				DHT.logInfo("new node "+newEntry+" claims same ID or IP as "+existing+", might be impersonation attack or IP change. ignoring until old entry times out");
				return;
			}
		}
		
		
		if (entriesRef.size() < DHTConstants.MAX_ENTRIES_PER_BUCKET)
		{
			// insert if not already in the list and we still have room
			modifyMainBucket(null,newEntry);
			return;
		}

		if (replaceBadEntry(newEntry))
			return;

		KBucketEntry youngest = entriesRef.get(entriesRef.size()-1);

		// older entries displace younger ones (although that kind of stuff should probably go through #modifyMainBucket directly)
		// entries with a 2.5times lower RTT than the current youngest one displace the youngest. safety factor to prevent fibrilliation due to changing RTT-estimates / to only replace when it's really worth it
		if (youngest.getCreationTime() > newEntry.getCreationTime() || newEntry.getRTT() * 2.5 < youngest.getRTT())
		{
			modifyMainBucket(youngest,newEntry);
			// it was a useful entry, see if we can use it to replace something questionable
			insertInReplacementBucket(youngest);
			return;
		}
		
		insertInReplacementBucket(newEntry);
	}
	
	
	/**
	 * mostly meant for internal use or transfering entries into a new bucket.
	 * to update a bucket properly use {@link #insertOrRefresh(KBucketEntry)}
	 */
	public void modifyMainBucket(KBucketEntry toRemove, KBucketEntry toInsert) {
	
		// we're synchronizing all modifications, therefore we can freely reference the old entry list, it will not be modified concurrently
		synchronized (this)
		{
			if(toInsert != null && entries.stream().anyMatch(toInsert::matchIPorID))
				return;
			
			List<KBucketEntry> newEntries = new ArrayList<KBucketEntry>(entries);
			boolean removed = false;
			boolean added = false;
			
			// removal never violates ordering constraint, no checks required
			if(toRemove != null)
				removed = newEntries.remove(toRemove);
			
			

			if(toInsert != null)
			{
				int oldSize = newEntries.size();
				boolean wasFull = oldSize >= DHTConstants.MAX_ENTRIES_PER_BUCKET;
				KBucketEntry youngest = oldSize > 0 ? newEntries.get(oldSize-1) : null;
				boolean unorderedInsert = youngest != null && toInsert.getCreationTime() < youngest.getCreationTime();
				added = !wasFull || unorderedInsert;
				if(added)
				{
					newEntries.add(toInsert);
				} else
				{
					insertInReplacementBucket(toInsert);
				}
				
				if(unorderedInsert)
					Collections.sort(newEntries,KBucketEntry.AGE_ORDER);
				
				if(wasFull && added)
					while(newEntries.size() > DHTConstants.MAX_ENTRIES_PER_BUCKET)
						insertInReplacementBucket(newEntries.remove(newEntries.size()-1));
			}
			
			// make changes visible
			if(added || removed)
				entries = newEntries;
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
	
	public int getNumReplacements() {
		int c = 0;
		for(int i=0;i<replacementBucket.length();i++)
			if(replacementBucket.get(i) != null)
				c++;
		return c;
	}

	/**
	 * @return the entries
	 */
	public List<KBucketEntry> getEntries () {
		return new ArrayList<KBucketEntry>(entries);
	}
	
	public Stream<KBucketEntry> entriesStream() {
		return entries.stream();
	}
	
	public List<KBucketEntry> getReplacementEntries() {
		List<KBucketEntry> repEntries = new ArrayList<KBucketEntry>(replacementBucket.length());
		int current = currentReplacementPointer.get();
		for(int i=1;i<=replacementBucket.length();i++)
		{
			KBucketEntry e = replacementBucket.get((current + i) % replacementBucket.length());
			if(e != null)
				repEntries.add(e);
		}
		return repEntries;
	}

	/**
	 * A peer failed to respond
	 * @param addr Address of the peer
	 */
	public boolean onTimeout (InetSocketAddress addr) {
		List<KBucketEntry> entriesRef = entries;
		for (int i = 0, n=entriesRef.size(); i < n; i++)
		{
			KBucketEntry e = entriesRef.get(i);
			if (e.getAddress() == addr)
			{
				e.signalRequestTimeout();
				//only removes the entry if it is bad
				removeEntryIfBad(e, false);
				return true;
			}
		}
		return false;
	}

	/**
	 * Check if the bucket needs to be refreshed
	 *
	 * @return true if it needs to be refreshed
	 */
	public boolean needsToBeRefreshed () {
		long now = System.currentTimeMillis();

		return (now - lastRefresh > DHTConstants.BUCKET_REFRESH_INTERVAL && entries.stream().anyMatch(KBucketEntry::needsPing));
	}

	/**
	 * Resets the last modified for this Bucket
	 */
	public void updateRefreshTimer () {
		lastRefresh = System.currentTimeMillis();
	}
	

	@Override
	public String toString() {
		return "entries: "+entries+" replacements: "+replacementBucket;
	}

	/**
	 * Tries to instert entry by replacing a bad entry.
	 *
	 * @param entry Entry to insert
	 * @return true if replace was successful
	 */
	private boolean replaceBadEntry (KBucketEntry entry) {
		List<KBucketEntry> entriesRef = entries;
		for (int i = 0,n=entriesRef.size();i<n;i++) {
			KBucketEntry e = entriesRef.get(i);
			if (e.needsReplacement()) {
				// bad one get rid of it
				modifyMainBucket(e, entry);
				return true;
			}
		}
		return false;
	}
	
	private KBucketEntry getBestReplacementEntry()
	{
		while(true) {
			int bestIndex = -1;
			KBucketEntry bestFound = null;
			
			for(int i=0;i<replacementBucket.length();i++) {
				KBucketEntry entry = replacementBucket.get(i);
				if(entry == null)
					continue;
				boolean isBetter = bestFound == null || entry.getRTT() < bestFound.getRTT() || (entry.getRTT() == bestFound.getRTT() && entry.getLastSeen() > bestFound.getLastSeen());
				
				if(isBetter) {
					bestFound = entry;
					bestIndex = i;
				}
			}
			
			if(bestFound == null)
				return null;
			
			int newPointer = bestIndex-1;
			if(newPointer < 0)
				newPointer = replacementBucket.length()-1;
			if(replacementBucket.compareAndSet(bestIndex, bestFound, null)) {
				currentReplacementPointer.set(newPointer);
				return bestFound;
			}
		}
	}
	
	private void insertInReplacementBucket(KBucketEntry toInsert)
	{
		if(toInsert == null)
			return;
		
		outer:
		while(true)
		{
			int current = currentReplacementPointer.get();
			int insertationPoint = (current+1) % replacementBucket.length();
			
			KBucketEntry nextEntry = replacementBucket.get(insertationPoint);
			if(nextEntry == null || toInsert.getLastSeen() - nextEntry.getLastSeen() > 1000 || toInsert.getRTT() < nextEntry.getRTT())
			{
				for(int i=0;i<replacementBucket.length();i++)
				{
					// don't insert if already present
					KBucketEntry potentialDuplicate = replacementBucket.get(i);
					if(toInsert.matchIPorID(potentialDuplicate)) {
						if(toInsert.equals(potentialDuplicate))
							potentialDuplicate.mergeInTimestamps(toInsert);
						break outer;
					}
						
				}
				if(currentReplacementPointer.compareAndSet(current, insertationPoint))
				{
					replacementBucket.set(insertationPoint, toInsert);
					break;
				}
			} else
			{
				break;
			}
		}
	}
	
	/**
	 * only removes one bad entry at a time to prevent sudden flushing of the routing table
	 */
	public void checkBadEntries() {
		List<KBucketEntry> entriesRef = entries;
		KBucketEntry toRemove = entriesRef.stream().filter(KBucketEntry::needsReplacement).findAny().orElse(null);
		
		if (toRemove == null)
			return;
		
		KBucketEntry replacement = getBestReplacementEntry();
		if (replacement != null)
			modifyMainBucket(toRemove, replacement);
	}
	
	public Optional<KBucketEntry> findByIPorID(InetAddress ip, Key id) {
		return entries.stream().filter(e -> e.getID().equals(id) || e.getAddress().getAddress().equals(ip)).findAny();
	}
	
	public Optional<KBucketEntry> randomEntry() {
		return Optional.of(entries).filter(l -> !l.isEmpty()).map(l -> l.get(ThreadLocalRandom.current().nextInt(l.size())));
	}
	
	public void notifyOfResponse(MessageBase msg)
	{
		if(msg.getType() != Type.RSP_MSG || msg.getAssociatedCall() == null)
			return;
		List<KBucketEntry> entriesRef = entries;
		for (int i=0, n = entriesRef.size();i<n;i++)
		{
			KBucketEntry entry = entriesRef.get(i);
			
			// update last responded. insert will be invoked soon, thus we don't have to do the move-to-end stuff
			if(entry.getID().equals(msg.getID()))
			{
				entry.signalResponse(msg.getAssociatedCall().getRTT());
				return;
			}
		}
	}


	/**
	 * @param toRemove Entry to remove, if its bad
	 * @param force if true entry will be removed regardless of its state
	 */
	public void removeEntryIfBad(KBucketEntry toRemove, boolean force) {
		List<KBucketEntry> entriesRef = entries;
		if (entriesRef.contains(toRemove) && (force || toRemove.needsReplacement()))
		{
			KBucketEntry replacement = null;
			replacement = getBestReplacementEntry();

			// only remove if we have a replacement or really need to
			if(replacement != null || force)
				modifyMainBucket(toRemove,replacement);
		}
		
	}

	/**
	 * @param node the node to set
	 */
	public void setNode (Node node) {
		this.node = node;
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		Map<String,Object> serialized = (Map<String, Object>) in.readObject();
		Object obj = serialized.get("mainBucket");
		if(obj instanceof Collection<?>)
			entries.addAll((Collection<KBucketEntry>)obj);
		obj = serialized.get("replacementBucket");
		if(obj instanceof Collection<?>)
		{
			// we are possibly violating the last-seen ordering of the replacement bucket here
			// but since we're probably deserializing old data that's hardly relevant
			for(KBucketEntry e : (Collection<KBucketEntry>)obj)
				insertInReplacementBucket(e);
		}
			
		obj = serialized.get("lastModifiedTime");
		if(obj instanceof Long)
			lastRefresh = (Long)obj;
		obj = serialized.get("prefix");
		
		entries.removeAll(Collections.singleton(null));
		Collections.sort(entries, KBucketEntry.AGE_ORDER);
		//Collections.sort(replacementBucket,KBucketEntry.LAST_SEEN_ORDER);
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		Map<String,Object> serialized = new HashMap<String, Object>();
		// put entries as any type of collection, will convert them on deserialisation
		serialized.put("mainBucket", entries);
		serialized.put("replacementBucket", getReplacementEntries());
		serialized.put("lastModifiedTime", lastRefresh);

		out.writeObject(serialized);
	}
}
