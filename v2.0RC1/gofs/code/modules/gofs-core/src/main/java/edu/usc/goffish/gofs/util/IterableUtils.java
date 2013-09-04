/*
 *    Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License. 
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package edu.usc.goffish.gofs.util;

import java.util.*;

public final class IterableUtils {

	private IterableUtils() {
		throw new UnsupportedOperationException();
	}

	public static int iterableCount(Iterable<?> i) {
		int count = 0;
		Iterator<?> it = i.iterator();
		while (it.hasNext()) {
			it.next();
			count++;
		}

		return count;
	}

	public static <E> boolean iterableContains(Iterable<? extends E> i, E match) {
		for (E e : i) {
			if (Objects.equals(e, match)) {
				return true;
			}
		}

		return false;
	}

	public static <E> List<E> toList(Iterable<? extends E> i) {
		return toList(i, 10);
	}

	public static <E> List<E> toList(Iterable<? extends E> i, int sizeHint) {
		ArrayList<E> list = new ArrayList<>(sizeHint);
		for (E e : i) {
			list.add(e);
		}
		list.trimToSize();
		return list;
	}

	public static <E> Iterable<E> filterIterable(Iterable<? extends E> i, IteratorFilter<E> f) {
		return new FilterIterable<>(i, f);
	}

	public static <E1, E2> Iterable<E2> transformIterable(Iterable<? extends E1> i, IteratorTransform<E1, E2> t) {
		return new TransformIterable<>(i, t);
	}

	public static <E> Iterable<E> unmodifiableIterable(Iterable<E> i) {
		if (i instanceof ReadOnlyIterable) {
			return i;
		}

		return new ReadOnlyIterable<>(i);
	}

	public static interface IteratorFilter<E> {

		boolean filter(E e);
	}

	public static interface IteratorTransform<E1, E2> {

		E2 transform(E1 e);
	}

	private static class ReadOnlyIterable<E> implements Iterable<E> {

		private final Iterable<E> _iterable;

		public ReadOnlyIterable(Iterable<E> iterable) {
			if (iterable == null) {
				throw new IllegalArgumentException();
			}

			_iterable = iterable;
		}

		@Override
		public Iterator<E> iterator() {
			return new ReadOnlyIterator<>(_iterable.iterator());
		}
	}

	private static class FilterIterable<E> implements Iterable<E> {

		private final Iterable<? extends E> _iterable;
		private final IteratorFilter<E> _filter;

		public FilterIterable(Iterable<? extends E> iterable, IteratorFilter<E> filter) {
			if (iterable == null) {
				throw new IllegalArgumentException();
			}
			if (filter == null) {
				throw new IllegalArgumentException();
			}

			_iterable = iterable;
			_filter = filter;
		}

		@Override
		public Iterator<E> iterator() {
			return new FilterIterator<>(_iterable.iterator(), _filter);
		}
	}

	private static class TransformIterable<E1, E2> implements Iterable<E2> {

		private final Iterable<? extends E1> _iterable;
		private final IteratorTransform<E1, E2> _transform;

		public TransformIterable(Iterable<? extends E1> iterable, IteratorTransform<E1, E2> transform) {
			if (iterable == null) {
				throw new IllegalArgumentException();
			}
			if (transform == null) {
				throw new IllegalArgumentException();
			}

			_iterable = iterable;
			_transform = transform;
		}

		@Override
		public Iterator<E2> iterator() {
			return new TransformIterator<>(_iterable.iterator(), _transform);
		}
	}
}
