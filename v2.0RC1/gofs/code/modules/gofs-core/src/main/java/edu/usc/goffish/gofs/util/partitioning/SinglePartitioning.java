package edu.usc.goffish.gofs.util.partitioning;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;

import java.util.*;

import edu.usc.goffish.gofs.graph.*;

public final class SinglePartitioning extends AbstractLong2IntMap implements IPartitioning {

	private static final long serialVersionUID = -4981585851945330867L;

	private static final int PARTITION_ID = 1;
	private static final IntSet PARTITIONS = IntSets.unmodifiable(new IntArraySet(new int[]{ PARTITION_ID }));

	private final IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> _graph;

	public SinglePartitioning(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph) {
		if (graph == null) {
			throw new IllegalArgumentException();
		}

		_graph = graph;
	}

	@Override
	public ObjectSet<Entry> long2IntEntrySet() {
		return new EntrySet();
	}

	@Override
	public int get(long key) {
		return PARTITION_ID;
	}

	@Override
	public int size() {
		return _graph.numVertices();
	}

	@Override
	public IntSet getPartitions() {
		return PARTITIONS;
	}

	private final class EntrySet extends AbstractObjectSet<Long2IntMap.Entry> implements FastEntrySet {

		@Override
		public ObjectIterator<Long2IntMap.Entry> iterator() {
			return new AbstractObjectIterator<Long2IntMap.Entry>() {

				private final Iterator<? extends IIdentifiableVertex> next = _graph.vertices().iterator();

				public boolean hasNext() {
					return next.hasNext();
				}

				public Entry next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}

					return new SinglePartitionEntry(next.next().getId(), PARTITION_ID);
				}
			};
		}

		public ObjectIterator<Long2IntMap.Entry> fastIterator() {
			return new AbstractObjectIterator<Long2IntMap.Entry>() {

				private final Iterator<? extends IIdentifiableVertex> next = _graph.vertices().iterator();
				private final SinglePartitionEntry entry = new SinglePartitionEntry(0L, PARTITION_ID);

				public boolean hasNext() {
					return next.hasNext();
				}

				public Entry next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}

					entry.internalSetKey(next.next().getId());
					return entry;
				}
			};
		}

		public int size() {
			return _graph.numVertices();
		}

		@SuppressWarnings("unchecked")
		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			final Map.Entry<Long, Integer> e = (Map.Entry<Long, Integer>)o;
			final long k = ((e.getKey()).longValue());
			return SinglePartitioning.this.containsKey(k) && (SinglePartitioning.this.get(k) == PARTITION_ID);
		}
	}

	private static final class SinglePartitionEntry extends BasicEntry {

		public SinglePartitionEntry(long key, int value) {
			super(key, value);
		}

		protected void internalSetKey(long key) {
			this.key = key;
		}
	}
}
