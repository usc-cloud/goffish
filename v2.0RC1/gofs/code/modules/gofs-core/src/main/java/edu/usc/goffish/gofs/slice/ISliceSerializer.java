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

/**
 * All implementations of ISliceSerializer must also provide a default no-arg constructor in order to be used with GoFS
 * deployments, as well as be thread-safe for use from multiple threads.
 */
public interface ISliceSerializer {

	public OutputStream prepareStream(OutputStream stream) throws IOException;

	public InputStream prepareStream(InputStream stream) throws IOException;

	public long serialize(ISlice slice, OutputStream stream) throws IOException;

	public <T extends ISlice> T deserialize(InputStream stream, Class<T> deserializedClass) throws IOException;
}
