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

public abstract class AbstractWrapperIterator<E> implements Iterator<E> {

	private E _next;
	private boolean _outOfDate;

	protected AbstractWrapperIterator() {
		_next = null;
		_outOfDate = true;
	}

	/***
	 * Should be implemented by subclasses to return the next value this
	 * iterator will return or null if there are no more elements. The
	 * implementation of this class guarantees that if the subclass is using an
	 * iterator-like structure as an element provider, and it does not advance
	 * this iterator past what is necessary to return the next element, then the
	 * implementation of {@link #remove()} is a trivial pass through call to the
	 * backing iterator. In other words advanceToNext is guaranteed to be called
	 * only once per call to {@link #next()}, and it is guaranteed to be called
	 * before a call to {@link #next()} which returns that element.
	 * 
	 * @return the next element {@link #next()} should return, or null if there
	 *         are no more elements
	 */
	protected abstract E advanceToNext();

	@Override
	public boolean hasNext() {
		if (_outOfDate) {
			_next = advanceToNext();
			_outOfDate = false;
		}

		return _next != null;
	}

	@Override
	public E next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		_outOfDate = true;
		return _next;
	}

	@Override
	public abstract void remove();
}
