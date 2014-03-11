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
package edu.usc.goffish.gofs.namenode;

import java.io.*;
import java.net.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;

public interface IInternalNameNode extends INameNode {

	/**
	 * Adds a data node to the set of data nodes this name node is responsible for.
	 * 
	 * @param dataNodes
	 *            the data node to add
	 */
	void addDataNode(URI dataNode) throws IOException;

	/**
	 * Clears the list of data nodes this name node is responsible. Note that this does not affect the partition
	 * directory at all, and if used, may lead to the partition directory being out of sync with this name node.
	 * 
	 * @throws IOException
	 */
	void clearDataNodes() throws IOException;

	/**
	 * Returns the ISliceSerializer all slices in this name node must be serialized with.
	 * 
	 * @return the ISliceSerializer all slices in this name node must be serialized with
	 * @throws IOException
	 */
	ISliceSerializer getSerializer() throws IOException;
	
	/**
	 * Sets the type of ISliceSerializer all slices in this name node must be serialized with.
	 * 
	 * @param sliceSerializer
	 *            the type of ISliceSerializer all slices in this name node must be serialized with
	 * @throws IOException
	 */
	void setSerializer(Class<? extends ISliceSerializer> sliceSerializer) throws IOException;

}
