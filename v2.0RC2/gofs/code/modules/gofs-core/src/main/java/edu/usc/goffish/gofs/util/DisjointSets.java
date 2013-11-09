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

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

public final class DisjointSets<T> {

	// map of elements to indices in parents array
	private final Object2IntOpenHashMap<T> _elements;

	// every index points to parent of that index. if value is negative, that index is a set root, and the absolute
	// value is the rank
	private final IntArrayList _parents;

	private int _numSets;

	public DisjointSets(int initialCapacity) {
		_elements = new Object2IntOpenHashMap<T>(initialCapacity, 1f);
		_parents = new IntArrayList(initialCapacity);
		_numSets = 0;
	}

	public void addSet(T e) {
		_elements.put(e, _parents.size());
		_parents.add(-1);
		_numSets++;
	}

	/**
	 * Unions the sets of the respective elements.
	 * 
	 * @param x
	 *            An element in the first set to union
	 * @param y
	 *            An element in the second set to union
	 */
	public void union(T x, T y) {
		if (!_elements.containsKey(x) || !_elements.containsKey(y)) {
			throw new IllegalArgumentException();
		}

		union(_elements.getInt(x), _elements.getInt(y));
	}

	protected void union(int x, int y) {
		assert (x >= 0 && y >= 0);
		assert (x < _parents.size() && y < _parents.size());

		x = find(x);
		y = find(y);

		if (x == y) {
			return;
		}

		int xRank = _parents.getInt(x);
		int yRank = _parents.getInt(y);

		if (yRank < xRank) {
			_parents.set(x, y);
		} else {
			if (yRank == xRank) {
				_parents.set(x, xRank - 1);
			}
			_parents.set(y, x);
		}

		_numSets--;
	}

	protected int find(int x) {
		assert (x >= 0 && x < _parents.size());

		int parent = _parents.getInt(x);

		if (parent < 0) {
			return x;
		} else {
			parent = find(parent);
			_parents.set(x, parent);
			return parent;
		}
	}

	/**
	 * Returns the full collection of elements in the same set as the given elements. Includes the given element in the
	 * returned collection.
	 * 
	 * @param e
	 *            the element to find the owning set of
	 * @return the set of elements the given element belongs to
	 */
	public Collection<T> retrieveSet(T e) {
		if (!_elements.containsKey(e)) {
			throw new IllegalArgumentException();
		}

		int root = find(_elements.getInt(e));
		ArrayList<T> set = new ArrayList<>(Math.abs(_parents.getInt(root)));

		ObjectIterator<Object2IntMap.Entry<T>> it = _elements.object2IntEntrySet().fastIterator();
		while (it.hasNext()) {
			Object2IntMap.Entry<T> entry = it.next();
			if (find(entry.getIntValue()) == root) {
				set.add(entry.getKey());
			}
		}

		return set;
	}

	/**
	 * Returns a collection of all the sets, where each set is a collection of all the values in the set.
	 * 
	 * @return a collection of all the sets, where each set is a collection of all the values in the set
	 */
	public Collection<? extends Collection<T>> retrieveSets() {
		Int2ObjectMap<ArrayList<T>> sets = new Int2ObjectOpenHashMap<>(_numSets, 1f);

		for (int i = 0; i < _parents.size(); i++) {
			int r = _parents.getInt(i);
			if (r < 0) {
				sets.put(i, new ArrayList<T>(Math.abs(r)));
			}
		}

		ObjectIterator<Object2IntMap.Entry<T>> it = _elements.object2IntEntrySet().fastIterator();
		while (it.hasNext()) {
			Object2IntMap.Entry<T> entry = it.next();
			sets.get(find(entry.getIntValue())).add(entry.getKey());
		}

		return new ArrayList<>(sets.values());
	}

	public int numSets() {
		return _numSets;
	}
}
