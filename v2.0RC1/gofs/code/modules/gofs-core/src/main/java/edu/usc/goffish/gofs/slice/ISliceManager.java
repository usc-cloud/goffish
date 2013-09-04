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

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

/**
 * This interface represents a object capable of writing and reading partitions and their associated instance data. Each
 * slice manager is responsible for only a single partition, and assumes that it is the only slice manager responsible
 * for writing to the slices it manages. Multiple slice managers may read simultaneously, but any writing action will
 * result in unspecified behavior from all other readers/writers. The {@link #invalidate()} method may be used to inform
 * a slice manager that someone may have written to its slices, and forces it to invalidate any cached data and reread
 * it before any future read or write.
 */
public interface ISliceManager {

	/**
	 * Returns the UUID representing the partition this slice manager manages.
	 * 
	 * @return the UUID representing the partition this slice manager manages
	 */
	UUID getPartitionUUID();

	/**
	 * Returns the partition id of the partition this slice manager manages, or an invalid partition id if this
	 * partition has not been assigned a partition yet.
	 * 
	 * @return the partition id of the partition this slice manager manages, or an invalid partition id if this
	 *         partition has not been assigned a partition yet
	 */
	int getPartitionId();

	/**
	 * ISliceManager's normal assumption is that it is the only entity writing to the slices it manages. If this is not
	 * true, problems will arise. If for some reason there is another writer to the same slices, this method may be used
	 * to inform the slice manager that any cached information it has is invalid, and it MUST refresh the information at
	 * some point before the next read or write call. This does not affect any ongoing instance iterations, which will
	 * still have unspecified behavior, as mentioned in {@link #writeInstances(ISerializablePartition)}.
	 */
	void invalidate();

	/**
	 * Reads and returns the partition this manager is responsible for.
	 * 
	 * @return the partition this manager is responsible for
	 * @throws IOException
	 */
	IPartition readPartition() throws IOException;

	/**
	 * Writes only the template information for the given partition. The partition id must match any previous id, and
	 * any previous information will be overwritten.
	 * 
	 * @param partition
	 *            the partition to write
	 * @return the number of bytes written
	 * @throws IOException
	 */
	long writePartitionTemplate(IPartition partition) throws IOException;

	/**
	 * Writes the template and instances for the given partition. The partition id must match any previous id, and any
	 * previous information will be overwritten.
	 * 
	 * @param partition
	 *            the partition to write
	 * @param instanceGroupingSize
	 *            the number of instances to group per slice
	 * @param numSubgraphBins
	 *            the number of bins to use for subgraph packing
	 * @return the number of bytes written
	 * @throws IOException
	 */
	long writePartition(ISerializablePartition partition, int instancesGroupingSize, int numSubgraphBins) throws IOException;

	/**
	 * Completely deletes any stored information for this partition.
	 */
	void deletePartition() throws IOException;
}
