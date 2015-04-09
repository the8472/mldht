package the8472.utils;

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

}
