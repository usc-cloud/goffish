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
package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public interface IPartitionBuilder {

	/**
	 * Method should use the given partitioning to create partitions. These
	 * partitions will be returned through {@link #getPartitions()}. How work is
	 * divided between the methods is implementation dependent.
	 * 
	 * @param partitioning
	 *            the partitioning to build partitions from
	 * @throws IOException
	 */
	void buildPartitions(IPartitioning partitioning) throws IOException;

	/**
	 * Returns an Iterable of partitions that are suitable for writing to disk.
	 * It is encouraged that implementations return partitions from the Iterable
	 * on demand so that it is possible to keep only a single partition in
	 * memory at once.
	 * 
	 * @return an Iterable of partitions in a serializable format
	 * @throws IOException
	 */
	Iterable<ISerializablePartition> getPartitions() throws IOException;
}
