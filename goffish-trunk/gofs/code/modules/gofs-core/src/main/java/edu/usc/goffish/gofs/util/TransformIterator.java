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

import edu.usc.goffish.gofs.util.IterableUtils.IteratorTransform;

public class TransformIterator<E1, E2> extends AbstractWrapperIterator<E2> {

	private final Iterator<? extends E1> _iterator;
	private final IteratorTransform<E1, E2> _transform;

	public TransformIterator(Iterator<? extends E1> iterator, IteratorTransform<E1, E2> transform) {
		if (iterator == null) {
			throw new IllegalArgumentException();
		}
		if (transform == null) {
			throw new IllegalArgumentException();
		}

		_iterator = iterator;
		_transform = transform;
	}

	@Override
	protected E2 advanceToNext() {
		if (!_iterator.hasNext()) {
			return null;
		}

		return _transform.transform(_iterator.next());
	}

}
