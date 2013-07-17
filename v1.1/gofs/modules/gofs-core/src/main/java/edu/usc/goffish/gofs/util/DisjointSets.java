/*
*  Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
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

public final class DisjointSets<T> {

	private final HashMap<T, Integer> _ranks;
	private final HashMap<T, T> _parents;

	private int _numSets;

	public DisjointSets() {
		_ranks = new HashMap<T, Integer>();
		_parents = new HashMap<T, T>();
		_numSets = 0;
	}

	public DisjointSets(int initialCapacity) {
		_ranks = new HashMap<T, Integer>(2 * initialCapacity);
		_parents = new HashMap<T, T>(2 * initialCapacity);
		_numSets = 0;
	}

	public void makeSet(T e) {
		assert (!_parents.containsKey(e));

		_ranks.put(e, 0);
		_parents.put(e, e);
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
		assert (_parents.containsKey(x));
		assert (_parents.containsKey(y));

		x = find(x);
		y = find(y);

		if (x == y) {
			return;
		}

		int xRank = _ranks.get(x);
		int yRank = _ranks.get(y);

		if (xRank < yRank) {
			_parents.put(x, y);
		} else if (xRank > yRank) {
			_parents.put(y, x);
		} else {
			_parents.put(y, x);
			_ranks.put(x, xRank + 1);
		}

		_numSets--;
	}

	/**
	 * Finds the set an element belongs to, performing path compression along
	 * the way.
	 * 
	 * @param element
	 *            The element to find the set of
	 * @return the element representing the found set
	 */
	public T find(T element) {
		assert (_parents.containsKey(element));

		T parent = _parents.get(element);

		if (parent != element) {
			parent = find(parent);
			_parents.put(element, parent);
		}

		return parent;
	}

	public int numSets() {
		return _numSets;
	}
}
