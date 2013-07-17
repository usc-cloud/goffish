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
import java.net.*;

import edu.usc.goffish.gofs.*;

public interface IPartitionDistributer {

	/**
	 * This will distribute the given partition to the URI host at the URI path.
	 * Other URI information may be used, but this is implementation specific.
	 * 
	 * @param location
	 *            The location to distribute partition slices to
	 * @param partition
	 *            The partition to write
	 * @return The location URI with the partition metadata slice UUID appended
	 *         as the fragment
	 * @throws IOException
	 */
	URI distribute(URI location, ISerializablePartition partition) throws IOException;
}
