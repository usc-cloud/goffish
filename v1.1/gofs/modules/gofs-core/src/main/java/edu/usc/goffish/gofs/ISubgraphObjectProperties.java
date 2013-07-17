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

import java.util.*;

/**
 * This represents a set of property values for a single object, an edge or
 * vertex.
 */
public interface ISubgraphObjectProperties extends Iterable<String> {

	/**
	 * Returns the id of the subgraph object this class contains properties for.
	 * 
	 * @return the id of the subgraph object this class contains properties for
	 */
	long getId();

	/**
	 * Returns the set of properties this class may contain properties for.
	 * 
	 * @return the set of properties this class may contain properties for
	 */
	PropertySet getProperties();

	/**
	 * Returns if this subgraph object has a specified value for the given
	 * property, i.e., if this is a dynamic property with a non-default value.
	 * 
	 * @param propertyName
	 *            the name of the property
	 * @return true if this is a dynamic property with a non-default value
	 */
	boolean hasSpecifiedProperty(String propertyName);

	/**
	 * Returns all the properties this subgraph object has a specified value
	 * for, i.e. all the dynamic properties with non-default values.
	 * 
	 * @return the dynamic properties with non-default values
	 */
	Collection<String> getSpecifiedProperties();

	/**
	 * Returns the value of the given property for the subgraph object in this
	 * instance.
	 * 
	 * @param propertyName
	 *            the name of the property
	 * @return the value of the given property for the subgraph object in this
	 *         instance
	 */
	Object getValue(String propertyName);
}
