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
package edu.usc.goffish.gofs.graph;

public interface IEdge {

	IVertex getSource();

	IVertex getSink();

	/**
	 * This method is only meaningful in an undirected context, where it will
	 * return the source vertex given that the caller has passed in what they
	 * are treating as the sink vertex. In a directed context this is equivalent
	 * to {@link #getSource()}. If the context is unknown it is recommended to
	 * use this method instead of {@link #getSource()}.
	 * 
	 * @param assumed_sink
	 *            the vertex the caller is treating as the sink
	 * @return the source vertex given the assumed sink
	 */
	IVertex getSource(IVertex assumed_sink);

	/**
	 * This method is only meaningful in an undirected context, where it will
	 * return the sink vertex given that the caller has passed in what they are
	 * treating as the source vertex. In a directed context this is equivalent
	 * to {@link #getSink()}. If the context is unknown it is recommended to use
	 * this method instead of {@link #getSink()}.
	 * 
	 * @param assumed_source
	 *            the vertex the caller is treating as the source
	 * @return the sink vertex given the assumed source
	 */
	IVertex getSink(IVertex assumed_source);
}
