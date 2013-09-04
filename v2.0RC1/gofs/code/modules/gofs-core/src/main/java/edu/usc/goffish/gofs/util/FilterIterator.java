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

import edu.usc.goffish.gofs.util.IterableUtils.IteratorFilter;

public class FilterIterator<E> extends AbstractWrapperIterator<E> {

	private final Iterator<? extends E> _iterator;
	private final IteratorFilter<E> _filter;

	public FilterIterator(Iterator<? extends E> iterator, IteratorFilter<E> filter) {
		if (iterator == null) {
			throw new IllegalArgumentException();
		}
		if (filter == null) {
			throw new IllegalArgumentException();
		}

		_iterator = iterator;
		_filter = filter;
	}

	@Override
	protected E advanceToNext() {
		while (_iterator.hasNext()) {
			E next = _iterator.next();
			if (_filter.filter(next)) {
				return next;
			}
		}

		return null;
	}

}
