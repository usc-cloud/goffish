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

import java.io.*;
import java.util.*;

public final class CollectionUtils {

	private CollectionUtils() {
		throw new UnsupportedOperationException();
	}

	public static <E> void addAll(Collection<E> collection, Iterable<? extends E> iterable) {
		addAll(collection, iterable.iterator());
	}

	public static <E> void addAll(Collection<E> collection, Iterator<? extends E> iterator) {
		while (iterator.hasNext()) {
			collection.add(iterator.next());
		}
	}

	@SuppressWarnings("rawtypes")
	public static final NavigableSet EMPTY_NAVIGABLE_SET = new EmptyNavigableSet<>();

	@SuppressWarnings("unchecked")
	public static <T> NavigableSet<T> emptyNavigableSet() {
		return (NavigableSet<T>)EMPTY_NAVIGABLE_SET;
	}

	private static class EmptyNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E>, Serializable {

		private static final long serialVersionUID = -7466369086223685996L;

		@Override
		public Iterator<E> iterator() {
			return Collections.emptyIterator();
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public boolean contains(Object obj) {
			return false;
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			return c.isEmpty();
		}

		@Override
		public Object[] toArray() {
			return new Object[0];
		}

		@Override
		public <T> T[] toArray(T[] a) {
			if (a.length > 0)
				a[0] = null;
			return a;
		}

		@Override
		public Comparator<? super E> comparator() {
			return null;
		}

		@Override
		public E first() {
			return null;
		}

		@Override
		public E last() {
			return null;
		}

		@Override
		public E lower(E e) {
			return null;
		}

		@Override
		public E floor(E e) {
			return null;
		}

		@Override
		public E ceiling(E e) {
			return null;
		}

		@Override
		public E higher(E e) {
			return null;
		}

		@Override
		public E pollFirst() {
			return null;
		}

		@Override
		public E pollLast() {
			return null;
		}

		@Override
		public NavigableSet<E> descendingSet() {
			return this;
		}

		@Override
		public Iterator<E> descendingIterator() {
			return Collections.emptyIterator();
		}

		@Override
		public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
			return this;
		}

		@Override
		public NavigableSet<E> headSet(E toElement, boolean inclusive) {
			return this;
		}

		@Override
		public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
			return this;
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement) {
			return this;
		}

		@Override
		public SortedSet<E> headSet(E toElement) {
			return this;
		}

		@Override
		public SortedSet<E> tailSet(E fromElement) {
			return this;
		}

		private Object readResolve() {
			return EMPTY_NAVIGABLE_SET;
		}
	}

	public static <T> NavigableSet<T> unmodifiableNavigableSet(NavigableSet<T> navigableSet) {
		if (navigableSet instanceof UnmodifiableNavigableSet) {
			return navigableSet;
		} else {
			return new UnmodifiableNavigableSet<>(navigableSet);
		}
	}

	private static class UnmodifiableNavigableSet<E> implements NavigableSet<E>, Serializable {

		private static final long serialVersionUID = -4551557806316852233L;

		private final NavigableSet<E> _navigableSet;

		public UnmodifiableNavigableSet(NavigableSet<E> navigableSet) {
			if (navigableSet == null) {
				throw new IllegalArgumentException();
			}

			_navigableSet = navigableSet;
		}

		@Override
		public Comparator<? super E> comparator() {
			return _navigableSet.comparator();
		}

		@Override
		public E first() {
			return _navigableSet.first();
		}

		@Override
		public E last() {
			return _navigableSet.last();
		}

		@Override
		public int size() {
			return _navigableSet.size();
		}

		@Override
		public boolean isEmpty() {
			return _navigableSet.isEmpty();
		}

		@Override
		public boolean contains(Object o) {
			return _navigableSet.contains(o);
		}

		@Override
		public Object[] toArray() {
			return _navigableSet.toArray();
		}

		@Override
		public <T> T[] toArray(T[] a) {
			return _navigableSet.toArray(a);
		}

		@Override
		public boolean add(E e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(Collection<? extends E> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

		@Override
		public E lower(E e) {
			return _navigableSet.lower(e);
		}

		@Override
		public E floor(E e) {
			return _navigableSet.floor(e);
		}

		@Override
		public E ceiling(E e) {
			return _navigableSet.ceiling(e);
		}

		@Override
		public E higher(E e) {
			return _navigableSet.higher(e);
		}

		@Override
		public E pollFirst() {
			throw new UnsupportedOperationException();
		}

		@Override
		public E pollLast() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<E> iterator() {
			return new ReadOnlyIterator<>(_navigableSet.iterator());
		}

		@Override
		public NavigableSet<E> descendingSet() {
			return new UnmodifiableNavigableSet<>(_navigableSet.descendingSet());
		}

		@Override
		public Iterator<E> descendingIterator() {
			return new ReadOnlyIterator<>(_navigableSet.descendingIterator());
		}

		@Override
		public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
			return new UnmodifiableNavigableSet<>(_navigableSet.subSet(fromElement, fromInclusive, toElement, toInclusive));
		}

		@Override
		public NavigableSet<E> headSet(E toElement, boolean inclusive) {
			return new UnmodifiableNavigableSet<>(_navigableSet.headSet(toElement, inclusive));
		}

		@Override
		public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
			return new UnmodifiableNavigableSet<>(_navigableSet.tailSet(fromElement, inclusive));
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement) {
			return Collections.unmodifiableSortedSet(_navigableSet.subSet(fromElement, toElement));
		}

		@Override
		public SortedSet<E> headSet(E toElement) {
			return Collections.unmodifiableSortedSet(_navigableSet.headSet(toElement));
		}

		@Override
		public SortedSet<E> tailSet(E fromElement) {
			return Collections.unmodifiableSortedSet(_navigableSet.tailSet(fromElement));
		}
	}
}
