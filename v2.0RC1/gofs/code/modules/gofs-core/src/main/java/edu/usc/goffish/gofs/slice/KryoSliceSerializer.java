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

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import org.objenesis.strategy.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.serializers.*;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.TreeMapSerializer;

import de.javakaffee.kryoserializers.*;
import edu.usc.goffish.gofs.util.*;
import edu.usc.goffish.gofs.util.FastUtilsKryoSerializers.*;

/**
 * This Kryo configuration DOES NOT support maps with null keys or values.
 */
public final class KryoSliceSerializer implements ISliceSerializer {

	private static final int DEFAULT_BUFFER_SIZE = 16384;

	private final ThreadLocalKryo _kryo;
	private final int _bufferSize;

	public KryoSliceSerializer() {
		this(DEFAULT_BUFFER_SIZE);
	}

	public KryoSliceSerializer(int bufferSize) {
		_bufferSize = bufferSize;
		_kryo = new ThreadLocalKryo();
	}

	@Override
	public synchronized Output prepareStream(OutputStream stream) throws IOException {
		Output output = new Output(new byte[_bufferSize]);
		output.setOutputStream(stream);
		return output;
	}

	@Override
	public Input prepareStream(InputStream stream) throws IOException {
		Input input = new Input(_bufferSize);
		input.setInputStream(stream);
		return input;
	}

	@Override
	public long serialize(ISlice slice, OutputStream stream) throws IOException {
		assert (slice != null);

		if (!(stream instanceof Output)) {
			// stream must be prepared with prepareStream
			throw new IOException();
		}

		synchronized (stream) {
			Output output = (Output)stream;
			_kryo.get().writeObject(output, slice);
			return output.total();
		}
	}

	@Override
	public <T extends ISlice> T deserialize(InputStream stream, Class<T> deserializedClass) throws IOException {
		if (!(stream instanceof Input)) {
			// stream must be prepared with prepareStream
			throw new IOException();
		}

		synchronized (stream) {
			Input input = (Input)stream;
			return _kryo.get().readObject(input, deserializedClass);
		}
	}

	private final static class ThreadLocalKryo extends ThreadLocal<Kryo> {

		@Override
		protected Kryo initialValue() {
			// disallow nulls wherever possible
			Kryo kryo = new Kryo();
			kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());

			FastUtilsKryoSerializers.addDefaultSerializers(kryo);

			// TODO: double check we don't need to handle cyclic object graphs
			kryo.setReferences(false);
			kryo.setRegistrationRequired(true);

			// prepare default serializers to disallow null keys and values
			CollectionSerializer collectionSerializer = new CollectionSerializer();
			collectionSerializer.setElementsCanBeNull(false);

			TreeMapSerializer treeMapSerializer = new TreeMapSerializer();
			treeMapSerializer.setKeysCanBeNull(false);
			treeMapSerializer.setValuesCanBeNull(false);

			MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeysCanBeNull(false);
			mapSerializer.setValuesCanBeNull(false);

			// register in approx order from inner loops to outer loops
			kryo.register(UUID.class);
			kryo.register(PartitionSlice.EdgeTuple.class);
			kryo.register(PartitionSlice.VertexTuple.class);
			kryo.register(PartitionSlice.SubgraphTuple.class);
			kryo.register(PartitionSlice.PropertyTuple.class, new PartitionSlice.PropertyTuple.PropertyTupleSerializer());
			kryo.register(PartitionSlice.RemoteVertexInfo.class);
			kryo.register(InstancesMetadata.class);
			kryo.register(InstancesMetadata.InstanceMetadata.class);
			kryo.register(InstancesMetadata.InstanceTuple.class);
			kryo.register(VersioningSlice.class);

			kryo.register(ArrayList.class, collectionSerializer);
			kryo.register(HashMap.class, mapSerializer);
			kryo.register(TreeMap.class, treeMapSerializer);
			kryo.register(TreeSet.class, collectionSerializer);
			kryo.register(Long2IntOpenHashMap.class);
			kryo.register(Long2ObjectOpenHashMap.class, new Long2ObjectOpenHashMapSerializer<>(null, null, false));
			kryo.register(Long2ObjectAVLTreeMap.class, new Long2ObjectAVLTreeMapSerializer<>(null, null, false));
			kryo.register(Long2ObjectMaps.UnmodifiableMap.class);
			kryo.register(Long2IntMaps.UnmodifiableMap.class);
			UnmodifiableCollectionsSerializer.registerSerializers(kryo);

			kryo.register(ISlice.class);
			kryo.register(PartitionMetadataSlice.class);
			kryo.register(PartitionSlice.class);
			kryo.register(PartitionInstancesSlice.class);

			return kryo;
		}
	}
}
