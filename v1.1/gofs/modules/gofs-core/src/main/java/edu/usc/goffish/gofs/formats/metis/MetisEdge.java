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

package edu.usc.goffish.gofs.formats.metis;

import edu.usc.goffish.gofs.graph.*;

public class MetisEdge implements IEdge {
	// do not extend Edge to avoid overhead of casting

	private final MetisVertex _source;
	private final MetisVertex _sink;
	
	MetisEdge(MetisVertex source, MetisVertex sink) {
		if (source == null || sink == null) {
			throw new IllegalArgumentException();
		}
		
		_source = source;
		_sink = sink;
	}

	@Override
	public MetisVertex getSource() {
		return _source;
	}

	@Override
	public MetisVertex getSink() {
		return _sink;
	}

	@Override
	public MetisVertex getSource(IVertex assumed_sink) {
		assert (_sink.equals(assumed_sink) || _source.equals(assumed_sink));

		if (_source.equals(assumed_sink)) {
			return _sink;
		}
		return _source;
	}

	@Override
	public MetisVertex getSink(IVertex assumed_source) {
		assert (_sink.equals(assumed_source) || _source.equals(assumed_source));

		if (_sink.equals(assumed_source)) {
			return _source;
		}
		return _sink;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + _source + "->" + _sink + "]";
	}
}
