package edu.usc.goffish.gofs.util;

import java.io.*;
import java.util.*;

public final class CollectionsUtil {

	private CollectionsUtil() {
		throw new UnsupportedOperationException();
	}
	
	public static <T> NavigableSet<T> unmodifiableNavigableSet(NavigableSet<T> navigableSet) {
		return new UnmodifiableNavigableSet<>(navigableSet);
	}
	
	static class UnmodifiableNavigableSet<E> implements NavigableSet<E>, Serializable {

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
			return _navigableSet.higher(e);
		}

		@Override
		public E higher(E e) {
			// TODO Auto-generated method stub
			return null;
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
