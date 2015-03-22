package the8472.utils;

import java.util.function.Consumer;

public class Functional {
	
	public static <T> T tap(T obj, Consumer<T> c) {
		c.accept(obj);
		return obj;
	}
	
	public static interface ThrowingSupplier<T,E extends Throwable> {
		T get() throws E;
	}
	
	public static <T> T unchecked(ThrowingSupplier<T, ?> f) {
		try {
			return f.get();
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

}
