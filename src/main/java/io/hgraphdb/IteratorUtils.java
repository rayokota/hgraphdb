package io.hgraphdb;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class IteratorUtils {

    private IteratorUtils() {
    }

    public static <T> Iterator<List<T>> partition(final Iterator<T> iterator, final int size) {
        if (iterator == null) throw new NullPointerException();
        if (size <= 0) throw new IllegalArgumentException();
        return new Iterator<List<T>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<T> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Object[] array = new Object[size];
                int count = 0;
                for (; count < size && iterator.hasNext(); count++) {
                    array[count] = iterator.next();
                }
                for (int i = count; i < size; i++) {
                    array[i] = null;
                }

                @SuppressWarnings("unchecked") // we only put Ts in it
                List<T> list = Collections.unmodifiableList((List<T>) Arrays.asList(array));
                return list.subList(0, count);
            }
        };
    }
}
