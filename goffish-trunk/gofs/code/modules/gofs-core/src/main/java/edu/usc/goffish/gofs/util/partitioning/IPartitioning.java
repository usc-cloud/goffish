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

package edu.usc.goffish.gofs.util.partitioning;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;

/**
 * This class represents the result of a partitioning operation and contains
 * both the list of resultant partitions and a mapping from vertex id to
 * partition id.
 */
public interface IPartitioning extends Long2IntMap {

	/**
	 * Returns the set of partition ids for this partitioning.
	 * 
	 * @return the set of partition ids for this partitioning
	 */
	IntSet getPartitions();
}
