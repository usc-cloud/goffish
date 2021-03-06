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

package edu.usc.goffish.gofs.formats.gml;

import java.util.*;

import edu.usc.goffish.gofs.util.*;

class KVPIterator extends AbstractWrapperIterator<KeyValuePair> {

	private final Iterator<KeyValuePair> _listIterator;
	private final String _keyToMatch;

	public KVPIterator(Iterable<KeyValuePair> iterable, String keyToMatch) {
		if (iterable == null) {
			throw new IllegalArgumentException();
		}
		if (keyToMatch == null) {
			throw new IllegalArgumentException();
		}

		_listIterator = iterable.iterator();
		_keyToMatch = keyToMatch;
	}

	@Override
	protected KeyValuePair advanceToNext() {
		while (_listIterator.hasNext()) {
			KeyValuePair kvp = _listIterator.next();
			if (kvp.Key().equals(_keyToMatch)) {
				return kvp;
			}
		}

		return null;
	}
}