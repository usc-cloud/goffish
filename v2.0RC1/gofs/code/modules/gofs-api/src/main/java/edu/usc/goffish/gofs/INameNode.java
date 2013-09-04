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

package edu.usc.goffish.gofs;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * This interface represents a central authority over a cluster of data nodes. It keeps track of which data nodes it is
 * responsible for, what the serialization scheme for every data node is, and maintains a directory of partition
 * information for every data node.
 */
public interface INameNode {

	/**
	 * Returns the URI representing this name node.
	 * 
	 * @return the URI representing this name node
	 */
	URI getURI();

	/**
	 * Returns true if this name node is available (there are no problems) and false otherwise.
	 * 
	 * @return true if this name node is available (there are no problems) and false otherwise
	 */
	boolean isAvailable();

	/**
	 * Returns a set of URIs representing all the data nodes this name node is responsible for.
	 * 
	 * @return a set of URIs representing all the data nodes this name node is responsible for
	 * @throws IOException
	 */
	Set<URI> getDataNodes() throws IOException;

	/**
	 * Returns an IPartitionDirectory object which may be used to answer queries on the partitions this name node is
	 * responsible for.
	 * 
	 * @return an IPartitionDirectory object which may be used to answer queries on the partitions this name node is
	 *         responsible for
	 */
	IPartitionDirectory getPartitionDirectory();
}
