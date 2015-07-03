package the8472.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

public class CowSet<E> implements Set<E> {
	
	static final AtomicReferenceFieldUpdater<CowSet,HashMap> u = AtomicReferenceFieldUpdater.newUpdater(CowSet.class, HashMap.class, "backingStore");
	
	static final HashMap<?, Boolean> EMPTY = new HashMap<>();
	
	volatile HashMap<E,Boolean> backingStore = (HashMap<E, Boolean>) EMPTY;
	
	<T> T update(Function<HashMap<E,Boolean>, ? extends T> c) {
		HashMap<E, Boolean> current;
		final HashMap<E, Boolean> newMap = new HashMap<>();
		T ret;
		do {
			current = backingStore;
			newMap.clear();
			newMap.putAll(current);
			ret = c.apply(newMap);
		} while(!u.compareAndSet(this, current, newMap));
		
		return ret;
	}
	

	@Override
	public int size() {
		return backingStore.size();
	}

	@Override
	public boolean isEmpty() {
		return backingStore.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return backingStore.containsKey(o);
	}

	@Override
	public Iterator<E> iterator() {
		return backingStore.keySet().iterator();
	}

	@Override
	public Object[] toArray() {
		return backingStore.keySet().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return backingStore.keySet().toArray(a);
	}

	@Override
	public boolean add(E e) {
		return update(m -> m.putIfAbsent(e, Boolean.TRUE) == null);
	}

	@Override
	public boolean remove(Object o) {
		return update(m -> m.remove(o));
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void clear() {
		backingStore = (HashMap<E, Boolean>) EMPTY;
	}
	
	@Override
	public Stream<E> stream() {
		return backingStore.keySet().stream();
	}
	
	public Set<E> snapshot() {
		return Collections.unmodifiableSet(backingStore.keySet());
	}

}
