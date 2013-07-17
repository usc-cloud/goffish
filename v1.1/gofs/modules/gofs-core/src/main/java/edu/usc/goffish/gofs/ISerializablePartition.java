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
 * This interface represents a partition which is suitable for serialization by ISliceManager.
 */
public interface ISerializablePartition extends IPartition {

	/**
	 * This API has a somewhat complex signature which is related to its
	 * somewhat complex usage. A partition implementing this interface may be
	 * passed to ISliceManager, which will use this function to extract instance
	 * data to write to slices. Working outwards through the return types then,
	 * the inner most return type is a Map of subgraph id's to actual instances.
	 * It is not enforced that this need cover every subgraph that exists in the
	 * partition, ISliceManager will only serialize what it is given. Each
	 * Map<Long, ? extends ISubgraphInstance> should correspond to a single
	 * instance id. I.e., the first Map corresponds to instance 1 for all
	 * subgraphs, the next instance 2, etc... These Maps may be grouped into
	 * Lists, where each List must contain 1 or more Maps. A List of Maps
	 * corresponds to a group of instances that may all be serialized into the
	 * same slice, and later deserialized from the same slice. How many
	 * instances may be put into a single slice is primarily memory limited, and
	 * as the SliceManager is not aware of the memory characteristics of each
	 * slice, it is left to the implementor of this interface to decide how to
	 * group instances, and there is no restriction that each List must be of
	 * the same size. Finally, the function returns an Iterable which must
	 * iterate over the afore-mentioned Lists.
	 * 
	 * @return See exhaustive explanation above
	 */
	Iterable<List<Map<Long, ? extends ISubgraphInstance>>> getSubgraphsInstances();
}
