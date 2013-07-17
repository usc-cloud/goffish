/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import org.objenesis.strategy.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

import de.javakaffee.kryoserializers.*;

public class KryoSliceSerializer implements ISliceSerializer {

	private final Kryo kryo;

	public KryoSliceSerializer() {
		kryo = new Kryo();
		kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());

		kryo.setReferences(false);
		kryo.setRegistrationRequired(true);

		kryo.register(UUID.class);
		kryo.register(ArrayList.class);
		kryo.register(HashMap.class);
		kryo.register(TreeMap.class);
		kryo.register(TreeSet.class);

		kryo.register(ISlice.class);
		kryo.register(PartitionMetadataSlice.class);
		kryo.register(PartitionMetadataSlice.SubgraphInstancesInfo.class);
		kryo.register(PartitionMetadataSlice.InstanceInfo.class);
		kryo.register(PartitionMetadataSlice.InstanceTuple.class);
		kryo.register(PartitionSlice.class);
		kryo.register(PartitionSlice.SubgraphTuple.class);
		kryo.register(PartitionSlice.VertexTuple.class);
		kryo.register(PartitionSlice.EdgeTuple.class);
		kryo.register(PartitionSlice.PropertyTuple.class, new PartitionSlice.PropertyTuple.PropertyTupleSerializer());
		kryo.register(PartitionInstancesSlice.class);
		kryo.register(PartitionInstancesSlice.PropertyInstanceTuple.class);

		UnmodifiableCollectionsSerializer.registerSerializers(kryo);
		// TODO: add support for fastutils collections
	}

	@Override
	public long serialize(ISlice slice, OutputStream stream) throws IOException {
		assert (slice != null);

		Output output = new Output(stream);
		kryo.writeObject(output, slice);
		output.flush();

		return output.total();
	}

	@Override
	public <T extends ISlice> T deserialize(InputStream stream, Class<T> deserializedClass) throws IOException {
		return kryo.readObject(new Input(stream), deserializedClass);
	}
}
