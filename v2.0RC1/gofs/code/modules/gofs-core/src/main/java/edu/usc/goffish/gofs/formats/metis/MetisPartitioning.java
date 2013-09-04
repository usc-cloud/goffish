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

package edu.usc.goffish.gofs.formats.metis;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.util.partitioning.*;

public final class MetisPartitioning implements IPartitioning {

	private final IntSet _partitions;
	private final Long2IntMap _vertexPartitions;

	public MetisPartitioning(IntSet partitions, Long2IntMap vertexPartitions) {
		_partitions = IntSets.unmodifiable(partitions);
		_vertexPartitions = Long2IntMaps.unmodifiable(vertexPartitions);
	}

	@Override
	public IntSet getPartitions() {
		return _partitions;
	}

	public static MetisPartitioning read(InputStream metisInput) throws IOException {
		return readAndRenumber(metisInput, null);
	}
	
	public static MetisPartitioning readAndRenumber(InputStream metisInput, long[] renumbering) throws IOException {
		if (metisInput == null) {
			throw new IllegalArgumentException();
		}
		
		try (@SuppressWarnings("resource") BufferedReader input = new BufferedReader(new InputStreamReader(metisInput))) {
			IntSet partitions = new IntArraySet(4);
			Long2IntMap vertexMappings;
			if (renumbering != null) {
				vertexMappings = new Long2IntOpenHashMap(renumbering.length, 1f);
			} else {
				// much faster insertion with unknown size, convert later
				vertexMappings = new Long2IntAVLTreeMap();
			}

			String line;
			int vertexId = 1;
			while ((line = input.readLine()) != null) {
				// normalize to start partition ids at 1
				int partitionId = Integer.parseInt(line) + 1;

				partitions.add(partitionId);
				
				if (renumbering == null || renumbering.length == 1) {
					vertexMappings.put(vertexId, partitionId);
				} else {
					if (vertexId >= renumbering.length) {
						// renumbering was invalid for this partitioning
						throw new IllegalStateException();
					}
					
					vertexMappings.put(renumbering[vertexId], partitionId);
				}

				vertexId++;
			}

			if (!(vertexMappings instanceof Long2IntOpenHashMap)) {
				vertexMappings = new Long2IntOpenHashMap(vertexMappings, 1f);
			}
			return new MetisPartitioning(partitions, vertexMappings);
		} catch (NumberFormatException e) {
			throw new MetisFormatException(e);
		}
	}

	public static void write(Map<Long, Integer> vertexPartitions, OutputStream metisOutput) throws IOException {
		if (vertexPartitions == null) {
			throw new IllegalArgumentException();
		}
		if (metisOutput == null) {
			throw new IllegalArgumentException();
		}
		
		try (BufferedWriter output = new BufferedWriter(new OutputStreamWriter(metisOutput))) {
			long vertexId = 1;
			while (vertexPartitions.containsKey(vertexId)) {
				output.write(vertexPartitions.get(vertexId).toString());
				output.write(System.lineSeparator());
				vertexId++;
			}
		}
	}

	@Override
	public int size() {
		return _vertexPartitions.size();
	}

	@Override
	public boolean isEmpty() {
		return _vertexPartitions.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return _vertexPartitions.containsKey(key);
	}

	@Override
	public boolean containsKey(long key) {
		return _vertexPartitions.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return _vertexPartitions.containsValue(value);
	}

	@Override
	public boolean containsValue(int value) {
		return _vertexPartitions.containsValue(value);
	}

	@Override
	public Integer get(Object key) {
		return _vertexPartitions.get(key);
	}

	@Override
	public int get(long key) {
		return _vertexPartitions.get(key);
	}

	@Override
	public Integer put(Long key, Integer value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int put(long key, int value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int remove(long key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends Long, ? extends Integer> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public LongSet keySet() {
		return _vertexPartitions.keySet();
	}

	@Override
	public IntCollection values() {
		return _vertexPartitions.values();
	}

	@Override
	public ObjectSet<java.util.Map.Entry<Long, Integer>> entrySet() {
		return _vertexPartitions.entrySet();
	}

	@Override
	public ObjectSet<Entry> long2IntEntrySet() {
		return _vertexPartitions.long2IntEntrySet();
	}

	@Override
	public void defaultReturnValue(int rv) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int defaultReturnValue() {
		return _vertexPartitions.defaultReturnValue();
	}
}
