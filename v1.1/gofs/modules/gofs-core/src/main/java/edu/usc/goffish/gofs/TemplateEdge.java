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

package edu.usc.goffish.gofs;

import edu.usc.goffish.gofs.graph.*;

public class TemplateEdge implements IIdentifiableEdge, Comparable<TemplateEdge> {
	// do not extend Edge to avoid overhead of casting

	private final long _id;
	private final TemplateVertex _source;
	private final TemplateVertex _sink;

	public TemplateEdge(long id, TemplateVertex source, TemplateVertex sink) {
		if (source == null) {
			throw new IllegalArgumentException();
		}
		if (sink == null) {
			throw new IllegalArgumentException();
		}
		
		_id = id;
		_source = source;
		_sink = sink;
	}

	public long getId() {
		return _id;
	}

	@Override
	public TemplateVertex getSource() {
		return _source;
	}

	@Override
	public TemplateVertex getSink() {
		return _sink;
	}

	@Override
	public TemplateVertex getSource(IVertex assumed_sink) {
		assert (_sink.equals(assumed_sink) || _source.equals(assumed_sink));

		if (_source.equals(assumed_sink)) {
			return _sink;
		}
		return _source;
	}

	@Override
	public TemplateVertex getSink(IVertex assumed_source) {
		assert (_sink.equals(assumed_source) || _source.equals(assumed_source));

		if (_sink.equals(assumed_source)) {
			return _source;
		}
		return _sink;
	}

	@Override
	public int compareTo(TemplateEdge other) {
		return Long.compare(_id, other._id);
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int)(_id ^ (_id >>> 32));
		return result;
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof TemplateEdge))
			return false;

		TemplateEdge other = (TemplateEdge)obj;
		return _id == other._id;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "@" + _id + "[" + getSource() + "->" + getSink() + "]";
	}
}
