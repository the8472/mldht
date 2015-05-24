package the8472.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public class Functional {
	
	public static <T> T tap(T obj, Consumer<T> c) {
		c.accept(obj);
		return obj;
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
			throw new RuntimeException(e);
		}
	}
	
	public static <R,T> Function<T,R> unchecked(ThrowingFunction<R,T,?> f) {
		return (arg) -> {
			try {
				return f.apply(arg);
			} catch(Throwable e) {
				throw new RuntimeException(e);
			}
		};
	}
	
	
	public static <T> CompletionStage<List<T>> awaitAll(Collection<CompletionStage<T>> stages) {
		return stages.stream().map(st -> st.thenApply(t -> Collections.singletonList(t))).reduce(CompletableFuture.<List<T>>completedFuture(Collections.emptyList()), (f1, f2) -> {
			return f1.thenCombine(f2, (a, b) -> {
				return tap(new ArrayList<>(a), l -> l.addAll(b));
			});
		});
		
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


}
