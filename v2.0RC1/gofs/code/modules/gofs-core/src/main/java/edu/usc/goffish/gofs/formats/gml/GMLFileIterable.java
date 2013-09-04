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

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.util.*;

public class GMLFileIterable implements Iterable<InputStream> {

	private final List<Path> _paths;

	public GMLFileIterable(List<Path> paths) {
		_paths = new ArrayList<>(paths);
	}

	public GMLFileIterable(Path[] paths) {
		_paths = new ArrayList<>(Arrays.asList(paths));
	}

	public GMLFileIterable(URL[] paths) throws URISyntaxException {
		_paths = new ArrayList<>(paths.length);
		for (URL url : paths) {
			_paths.add(Paths.get(url.toURI()));
		}
	}

	public GMLFileIterable(String[] paths) {
		_paths = new ArrayList<>(paths.length);
		for (String str : paths) {
			_paths.add(Paths.get(str));
		}
	}

	@Override
	public Iterator<InputStream> iterator() {
		return new GMLFileIterator(_paths.iterator());
	}

	private static class GMLFileIterator extends AbstractWrapperIterator<InputStream> {

		private final Iterator<Path> _iterator;

		public GMLFileIterator(Iterator<Path> iterator) {
			_iterator = iterator;
		}

		@Override
		protected InputStream advanceToNext() {
			while (_iterator.hasNext()) {
				try {
					return Files.newInputStream(_iterator.next());
				} catch (IOException e) {
				}
			}

			return null;
		}

		@Override
		public void remove() {
			_iterator.remove();
		}
	}
}
