package lbms.plugins.mldht.indexer.assemblyline;

import java.util.concurrent.LinkedBlockingQueue;

public class SoftCapacityQueue<E> extends LinkedBlockingQueue<E> {
	
	final int capacity;

	public SoftCapacityQueue(int capacity) {
		super();
		this.capacity = capacity;
	}
	
	public int remainingCapacity() {
		return Math.max(0, capacity-size());
	}
	
	public boolean offer(E e) {
		if(capacity <= size())
			return false;
		return super.offer(e);
	};


}
