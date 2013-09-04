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
package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;
import java.net.*;

import edu.usc.goffish.gofs.partition.*;

public interface IPartitionMapper {

	/**
	 * This method maps a partition to a specific location. Generally speaking,
	 * the location URI should specify the host and path, though the details are
	 * implementation specific. This method is required to be idempotent,
	 * calling the method multiple times with the same argument should result in
	 * the same location returned.
	 * 
	 * @param partition
	 *            The partition to be written.
	 * @return The location the partition should be written to.
	 * @throws IOException
	 */
	URI getLocationForPartition(ISerializablePartition partition) throws IOException;
}
