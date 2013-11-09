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

package edu.usc.goffish.gofs.partition;

import edu.usc.goffish.gofs.*;

import it.unimi.dsi.fastutil.longs.*;

/**
 * This interface represents a partition which is suitable for serialization by ISliceManager.
 */
public interface ISerializablePartition extends IPartition {

	/**
	 * This method returns a view of the instances in this partition in a format suitable for serialization. The
	 * returned Iterable should iterate over every instance id in the partition, and for each instance, provide a map
	 * containing the specific instance for that instance id for every subgraph in the partition. Order of iteration is
	 * unspecified.
	 * 
	 * @return an Iterable over a map of subgraph id to instance for every instance in the partition
	 */
	Iterable<Long2ObjectMap<? extends ISubgraphInstance>> getSubgraphsInstances();
}
