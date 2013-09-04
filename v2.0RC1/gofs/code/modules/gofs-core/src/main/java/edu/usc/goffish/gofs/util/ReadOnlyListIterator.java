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

public class ReadOnlyListIterator<E> extends ReadOnlyIterator<E> implements ListIterator<E> {

	public ReadOnlyListIterator(ListIterator<E> iterator) {
		super(iterator);
	}

	@Override
	public boolean hasPrevious() {
		return ((ListIterator<E>)_iterator).hasPrevious();
	}

	@Override
	public E previous() {
		return ((ListIterator<E>)_iterator).previous();
	}

	@Override
	public int nextIndex() {
		return ((ListIterator<E>)_iterator).nextIndex();
	}

	@Override
	public int previousIndex() {
		return ((ListIterator<E>)_iterator).previousIndex();
	}

	@Override
	public void set(E e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(E e) {
		throw new UnsupportedOperationException();
	}
}
