package the8472.utils;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public class Functional {
	
	public static <T> T tap(T obj, Consumer<T> c) {
		c.accept(obj);
		return obj;
	}
	
	public static <T, E extends Throwable> T tapThrow(T obj, ThrowingConsumer<T, E> c) throws E  {
		c.accept(obj);
		return obj;
	}
	
	public static interface ThrowingConsumer<T,E extends Throwable> {
		void accept(T arg) throws E;
	}
	
	@FunctionalInterface
	public static interface ThrowingSupplier<T,E extends Throwable> {
		T get() throws E;
	}
	
	@FunctionalInterface
	public static interface ThrowingFunction<R, T, E extends Throwable> {
		R apply(T arg) throws E;
	}
	
	public static <T> T unchecked(ThrowingSupplier<T, ?> f) {
		try {
			return f.get();
		} catch (Throwable e) {
			throwAsUnchecked(e);
			return null;
		}
	}
	
	public static <IN, OUT> OUT sync(IN obj, Function<IN, OUT> supp) {
		synchronized(obj) {
			return supp.apply(obj);
		}
	}
	
	public static <R,T> Function<T,R> unchecked(ThrowingFunction<R,T,?> f) {
		return (arg) -> {
			try {
				return f.apply(arg);
			} catch(Throwable e) {
				throwAsUnchecked(e);
				return null;
			}
		};
	}
	
	public static <T> Consumer<T> uncheckedCons(ThrowingConsumer<T, ?> cons) {
		return (arg) -> {
			try {
				cons.accept(arg);
			} catch (Throwable e) {
				throwAsUnchecked(e);
				return;
			}
		};
	}
	
	
	public static <T> CompletionStage<List<T>> awaitAll(Collection<? extends CompletionStage<T>> stages) {
		return stages.stream().map(st -> st.thenApply(Collections::singletonList)).reduce(completedFuture(emptyList()), (f1, f2) -> f1.thenCombine(f2, (a, b) -> tap(new ArrayList<>(a), l -> l.addAll(b))));
	}
	
	public static <IN, OUT, EX extends Throwable> Function<IN, OUT> castOrThrow(Class<OUT> type, Function<IN, EX> ex) {
		return (in) -> {
			if(!type.isInstance(in))
				throwAsUnchecked(ex.apply(in));
			return type.cast(in);
		};
	}

	
	
	public static void throwAsUnchecked(Throwable t) {
	    Thrower.asUnchecked(t);
	}
	
	private static class Thrower {

		@SuppressWarnings("unchecked")
		static private <T extends Throwable> void asUnchecked(Throwable t) throws T {
		    throw (T) t;
		}
	}

	public static <K, T> Optional<T> typedGet(Map<K, ?> map, K key, Class<T> clazz) {
		return Optional.ofNullable(map.get(key)).filter(clazz::isInstance).map(clazz::cast);
	}


}
