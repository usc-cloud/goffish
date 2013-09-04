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
package edu.usc.goffish.gofs.util;

import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;

import java.lang.reflect.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class FastUtilsKryoSerializers {

	private FastUtilsKryoSerializers() {
		throw new UnsupportedOperationException();
	}
	
	public static void addDefaultSerializers(Kryo kryo) {
		kryo.addDefaultSerializer(Long2IntOpenHashMap.class, Long2IntOpenHashMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntRBTreeMap.class, Long2IntRBTreeMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntAVLTreeMap.class, Long2IntAVLTreeMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntMaps.UnmodifiableMap.class, UnmodifiableLong2IntMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntMaps.EmptyMap.class, EmptyLong2IntMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntMaps.Singleton.class, SingletonLong2IntMapSerializer.class);
		kryo.addDefaultSerializer(Long2IntMap.class, Long2IntMapSerializer.class);

		kryo.addDefaultSerializer(Long2ObjectOpenHashMap.class, Long2ObjectOpenHashMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectRBTreeMap.class, Long2ObjectRBTreeMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectAVLTreeMap.class, Long2ObjectAVLTreeMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectMaps.UnmodifiableMap.class, UnmodifiableLong2ObjectMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectMaps.EmptyMap.class, EmptyLong2ObjectMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectMaps.Singleton.class, SingletonLong2ObjectMapSerializer.class);
		kryo.addDefaultSerializer(Long2ObjectMap.class, Long2ObjectMapSerializer.class);
	}

	// Long2ObjectMap serializers
	
	public static class Long2ObjectMapSerializer<T extends Long2ObjectMap<V>, V> extends Serializer<T> {

		private final Class<? extends V> _valueConcreteType;
		private final Serializer<?> _valueSerializer;
		protected final boolean _valuesMayBeNull;
		
		private Class<?> _valueConcreteGenericType;
		
		protected Serializer<?> _currentSerializer;
		protected Class<?> _currentValueClass;
		
		public Long2ObjectMapSerializer() {
			this(null, null);
		}
		
		public Long2ObjectMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer) {
			this(valueConcreteType, valueSerializer, false);
		}
		
		public Long2ObjectMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer, boolean valuesMayBeNull) {
			if (valueConcreteType != null && valueSerializer == null) {
				throw new IllegalArgumentException();
			}
			
			_valueConcreteType = valueConcreteType;
			_valueSerializer = valueSerializer;
			_valueConcreteGenericType = null;
			_valuesMayBeNull = valuesMayBeNull;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void setGenerics(Kryo kryo, Class[] generics) {
			_valueConcreteGenericType = null;
			if (_valueConcreteType == null) {
				// if we don't have a concrete class, see if generics can provide us with one
				if (generics[0] != null && kryo.isFinal(generics[0])) {
					_valueConcreteGenericType = generics[0];
				}
			}
		}
		
		protected T create(Kryo kryo, Input input, Class<? extends T> type, int size) {
			return kryo.newInstance(type);
		}
		
		protected ObjectIterator<Long2ObjectMap.Entry<V>> iterator(T map) {
			return map.long2ObjectEntrySet().iterator();
		}
		
		protected void popSerializerAndValueClass(Kryo kryo) {
			_currentValueClass = _valueConcreteType;
			_currentSerializer = _valueSerializer;
			if (_currentValueClass == null && _valueConcreteGenericType != null) {
				_currentValueClass = _valueConcreteGenericType;
				_currentSerializer = kryo.getSerializer(_currentValueClass);
			}
			
			_valueConcreteGenericType = null;
		}
		
		@Override
		public void write(Kryo kryo, Output output, T object) {
			popSerializerAndValueClass(kryo);
			
			output.writeInt(object.size());
			
			ObjectIterator<Long2ObjectMap.Entry<V>> it = iterator(object);
			while (it.hasNext()) {
				Long2ObjectMap.Entry<V> entry = it.next();
				output.writeLong(entry.getLongKey());
				if (_currentSerializer != null) {
					if (_valuesMayBeNull) {
						kryo.writeObjectOrNull(output, entry.getValue(), _currentSerializer);
					} else {
						if (entry.getValue() == null) { throw new IllegalStateException(); }
						kryo.writeObject(output, entry.getValue(), _currentSerializer);
					}
				} else {
					kryo.writeClassAndObject(output, entry.getValue());
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public T read(Kryo kryo, Input input, Class<T> type) {
			popSerializerAndValueClass(kryo);

			int size = input.readInt();
			T map = create(kryo, input, type, size);
			
			kryo.reference(map);
			
			for (int i = 0; i < size; i++) {
				long key = input.readLong();
				V value;
				if (_currentSerializer != null) {
					if (_valuesMayBeNull) {
						value = (V)kryo.readObjectOrNull(input, _currentValueClass, _currentSerializer);
					} else {
						value = (V)kryo.readObject(input, _currentValueClass, _currentSerializer);
					}
				} else {
					value = (V)kryo.readClassAndObject(input);
				}
				
				map.put(key, value);
			}
			
			return map;
		}
	}

	public static class Long2ObjectOpenHashMapSerializer<V> extends Long2ObjectMapSerializer<Long2ObjectOpenHashMap<V>, V> {

		private static final Field FIELD_f;

		static {
			Field f = null;
			try {
				f = Long2ObjectOpenHashMap.class.getDeclaredField("f");
			} catch (NoSuchFieldException | SecurityException e) {
			}
			FIELD_f = f;
			FIELD_f.setAccessible(true);
		}
		
		private transient float _cached_f;
		
		public Long2ObjectOpenHashMapSerializer() {
			this(null, null, true);
		}
		
		public Long2ObjectOpenHashMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer, boolean containsNullValues) {
			super(valueConcreteType, valueSerializer, containsNullValues);
			
			if (FIELD_f == null) {
				throw new IllegalStateException();
			}
		}
		
		@Override
		protected Long2ObjectOpenHashMap<V> create (Kryo kryo, Input input, Class<? extends Long2ObjectOpenHashMap<V>> type, int size) {
			return new Long2ObjectOpenHashMap<V>(size, _cached_f);
		}
		
		@Override
		protected ObjectIterator<Long2ObjectMap.Entry<V>> iterator(Long2ObjectOpenHashMap<V> map) {
			return map.long2ObjectEntrySet().fastIterator();
		}
		
		@Override
		public void write(Kryo kryo, Output output, Long2ObjectOpenHashMap<V> object) {
			try {
				output.writeFloat(FIELD_f.getFloat(object));
				super.write(kryo, output, object);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public Long2ObjectOpenHashMap<V> read(Kryo kryo, Input input, Class<Long2ObjectOpenHashMap<V>> type) {
			_cached_f = input.readFloat();
			return super.read(kryo, input, type);
		}
	}
	
	public static abstract class Long2ObjectSortedMapSerializer<T extends Long2ObjectSortedMap<V>, V> extends Long2ObjectMapSerializer<T, V> {
		
		private transient LongComparator _cached_comparator;
		
		public Long2ObjectSortedMapSerializer() {
			this(null, null, true);
		}
		
		public Long2ObjectSortedMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer, boolean containsNullValues) {
			super(valueConcreteType, valueSerializer, containsNullValues);
		}
		
		@Override
		protected T create(Kryo kryo, Input input, Class<? extends T> type, int size) {
			return create(kryo, input, type, size, _cached_comparator);
		}
		
		protected abstract T create(Kryo kryo, Input input, Class<? extends T> type, int size, LongComparator comparator);
		
		@Override
		public void write(Kryo kryo, Output output, T object) {
			kryo.writeClassAndObject(output, object.comparator());
			super.write(kryo, output, object);
		}
		
		@Override
		public T read(Kryo kryo, Input input, Class<T> type) {
			_cached_comparator = (LongComparator)kryo.readClassAndObject(input);
			return super.read(kryo, input, type);
		}
	}
	
	public static class Long2ObjectRBTreeMapSerializer<V> extends Long2ObjectSortedMapSerializer<Long2ObjectRBTreeMap<V>, V> {
		
		public Long2ObjectRBTreeMapSerializer() {
			this(null, null, true);
		}
		
		public Long2ObjectRBTreeMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer, boolean containsNullValues) {
			super(valueConcreteType, valueSerializer, containsNullValues);
		}
		
		@Override
		protected Long2ObjectRBTreeMap<V> create(Kryo kryo, Input input, Class<? extends Long2ObjectRBTreeMap<V>> type, int size, LongComparator comparator) {
			return new Long2ObjectRBTreeMap<V>(comparator);
		}
	}
	
	public static class Long2ObjectAVLTreeMapSerializer<V> extends Long2ObjectSortedMapSerializer<Long2ObjectAVLTreeMap<V>, V> {
		
		public Long2ObjectAVLTreeMapSerializer() {
			this(null, null, true);
		}
		
		public Long2ObjectAVLTreeMapSerializer(Class<? extends V> valueConcreteType, Serializer<?> valueSerializer, boolean containsNullValues) {
			super(valueConcreteType, valueSerializer, containsNullValues);
		}
		
		@Override
		protected Long2ObjectAVLTreeMap<V> create(Kryo kryo, Input input, Class<? extends Long2ObjectAVLTreeMap<V>> type, int size, LongComparator comparator) {
			return new Long2ObjectAVLTreeMap<V>(comparator);
		}
	}

	public static class UnmodifiableLong2ObjectMapSerializer<T> extends Serializer<Long2ObjectMap<T>> {

		private static final Field FIELD_map;

		static {
			Field map = null;
			try {
				map = Long2ObjectMaps.UnmodifiableMap.class.getDeclaredField("map");
			} catch (NoSuchFieldException | SecurityException e) {
			}
			FIELD_map = map;
			FIELD_map.setAccessible(true);
		}

		public UnmodifiableLong2ObjectMapSerializer() {
			if (FIELD_map == null) {
				throw new IllegalStateException();
			}
		}

		@Override
		public void write(Kryo kryo, Output output, Long2ObjectMap<T> object) {
			if (!(object instanceof Long2ObjectMaps.UnmodifiableMap)) {
				throw new IllegalArgumentException();
			}

			try {
				kryo.writeClassAndObject(output, FIELD_map.get(object));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Long2ObjectMap<T> read(Kryo kryo, Input input, Class<Long2ObjectMap<T>> type) {
			return Long2ObjectMaps.unmodifiable((Long2ObjectMap<T>)kryo.readClassAndObject(input));
		}
	}

	
	public static class EmptyLong2ObjectMapSerializer<V> extends Serializer<Long2ObjectMaps.EmptyMap<V>> {

		public EmptyLong2ObjectMapSerializer() {
			super(false, true);
		}
		
		@Override
		public void write(Kryo kryo, Output output, Long2ObjectMaps.EmptyMap<V> object) {
		}

		@SuppressWarnings("unchecked")
		@Override
		public Long2ObjectMaps.EmptyMap<V> read(Kryo kryo, Input input, Class<Long2ObjectMaps.EmptyMap<V>> type) {
			return (Long2ObjectMaps.EmptyMap<V>)Long2ObjectMaps.EMPTY_MAP;
		}
	}
	
	public static class SingletonLong2ObjectMapSerializer<V> extends Long2ObjectMapSerializer<Long2ObjectMaps.Singleton<V>, V> {

		public SingletonLong2ObjectMapSerializer() {
			setImmutable(true);
		}
		
		@Override
		public void write(Kryo kryo, Output output, Long2ObjectMaps.Singleton<V> object) {
			popSerializerAndValueClass(kryo);
			
			assert(object.size() == 0);
			
			for (Long2ObjectMap.Entry<V> entry : object.long2ObjectEntrySet()) {
				output.writeLong(entry.getLongKey());
				if (_currentSerializer != null) {
					if (_valuesMayBeNull) {
						kryo.writeObjectOrNull(output, entry.getValue(), _currentSerializer);
					} else {
						kryo.writeObject(output, entry.getValue(), _currentSerializer);
					}
				} else {
					kryo.writeClassAndObject(output, entry.getValue());
				}
				break;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Long2ObjectMaps.Singleton<V> read(Kryo kryo, Input input, Class<Long2ObjectMaps.Singleton<V>> type) {
			popSerializerAndValueClass(kryo);
			
			long key = input.readLong();
			V value;
			if (_currentSerializer != null) {
				if (_valuesMayBeNull) {
					value = (V)kryo.readObjectOrNull(input, _currentValueClass, _currentSerializer);
				} else {
					value = (V)kryo.readObject(input, _currentValueClass, _currentSerializer);
				}
			} else {
				value = (V)kryo.readClassAndObject(input);
			}
			
			return (Long2ObjectMaps.Singleton<V>)Long2ObjectMaps.singleton(key, value);
		}
	}
	
	// Long2IntMap serializers
	
	public static class Long2IntMapSerializer<T extends Long2IntMap> extends Serializer<T> {
		
		protected T create(Kryo kryo, Input input, Class<? extends T> type, int size) {
			return kryo.newInstance(type);
		}
		
		protected ObjectIterator<Long2IntMap.Entry> iterator(T map) {
			return map.long2IntEntrySet().iterator();
		}
		
		@Override
		public void write(Kryo kryo, Output output, T object) {
			output.writeInt(object.size());
			
			ObjectIterator<Long2IntMap.Entry> it = iterator(object);
			while (it.hasNext()) {
				Long2IntMap.Entry entry = it.next();
				output.writeLong(entry.getLongKey());
				output.writeInt(entry.getIntValue());
			}
		}

		@Override
		public T read(Kryo kryo, Input input, Class<T> type) {
			int size = input.readInt();
			T map = create(kryo, input, type, size);
			
			for (int i = 0; i < size; i++) {
				map.put(input.readLong(), input.readInt());
			}
			
			return map;
		}
	}

	public static class Long2IntOpenHashMapSerializer extends Long2IntMapSerializer<Long2IntOpenHashMap> {

		private static final Field FIELD_f;

		static {
			Field f = null;
			try {
				f = Long2IntOpenHashMap.class.getDeclaredField("f");
			} catch (NoSuchFieldException | SecurityException e) {
			}
			FIELD_f = f;
			FIELD_f.setAccessible(true);
		}

		private transient float _cached_f;
		
		public Long2IntOpenHashMapSerializer() {
			if (FIELD_f == null) {
				throw new IllegalStateException();
			}
		}
		
		@Override
		protected Long2IntOpenHashMap create(Kryo kryo, Input input, Class<? extends Long2IntOpenHashMap> type, int size) {
			return new Long2IntOpenHashMap(size, _cached_f);
		}

		@Override
		protected ObjectIterator<Long2IntMap.Entry> iterator(Long2IntOpenHashMap map) {
			return map.long2IntEntrySet().fastIterator();
		}

		@Override
		public void write(Kryo kryo, Output output, Long2IntOpenHashMap object) {
			try {
				output.writeFloat(FIELD_f.getFloat(object));
				super.write(kryo, output, object);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Long2IntOpenHashMap read(Kryo kryo, Input input, Class<Long2IntOpenHashMap> type) {
			_cached_f = input.readFloat();
			return super.read(kryo, input, type);
		}
	}
	
	public static abstract class Long2IntSortedMapSerializer<T extends Long2IntSortedMap> extends Long2IntMapSerializer<T> {
		
		private transient LongComparator _cached_comparator;
		
		@Override
		protected T create(Kryo kryo, Input input, Class<? extends T> type, int size) {
			return create(kryo, input, type, size, _cached_comparator);
		}
		
		protected abstract T create(Kryo kryo, Input input, Class<? extends T> type, int size, LongComparator comparator);
		
		@Override
		public void write(Kryo kryo, Output output, T object) {
			kryo.writeClassAndObject(output, object.comparator());
			super.write(kryo, output, object);
		}
		
		@Override
		public T read(Kryo kryo, Input input, Class<T> type) {
			_cached_comparator = (LongComparator)kryo.readClassAndObject(input);
			return super.read(kryo, input, type);
		}
	}
	
	public static class Long2IntRBTreeMapSerializer extends Long2IntSortedMapSerializer<Long2IntRBTreeMap> {
		
		@Override
		protected Long2IntRBTreeMap create(Kryo kryo, Input input, Class<? extends Long2IntRBTreeMap> type, int size, LongComparator comparator) {
			return new Long2IntRBTreeMap(comparator);
		}
	}
	
	public static class Long2IntAVLTreeMapSerializer extends Long2IntSortedMapSerializer<Long2IntAVLTreeMap> {
		
		@Override
		protected Long2IntAVLTreeMap create(Kryo kryo, Input input, Class<? extends Long2IntAVLTreeMap> type, int size, LongComparator comparator) {
			return new Long2IntAVLTreeMap(comparator);
		}
	}
	
	public static class UnmodifiableLong2IntMapSerializer extends Serializer<Long2IntMap> {

		private static final Field FIELD_map;

		static {
			Field map = null;
			try {
				map = Long2IntMaps.UnmodifiableMap.class.getDeclaredField("map");
			} catch (NoSuchFieldException | SecurityException e) {
			}
			FIELD_map = map;
			FIELD_map.setAccessible(true);
		}

		public UnmodifiableLong2IntMapSerializer() {
			super(false, true);

			if (FIELD_map == null) {
				throw new IllegalStateException();
			}
		}

		@Override
		public void write(Kryo kryo, Output output, Long2IntMap object) {
			if (!(object instanceof Long2IntMaps.UnmodifiableMap)) {
				throw new IllegalArgumentException();
			}

			try {
				kryo.writeClassAndObject(output, FIELD_map.get(object));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Long2IntMap read(Kryo kryo, Input input, Class<Long2IntMap> type) {
			return Long2IntMaps.unmodifiable((Long2IntMap)kryo.readClassAndObject(input));
		}
	}
	
	public static class EmptyLong2IntMapSerializer extends Serializer<Long2IntMaps.EmptyMap> {

		public EmptyLong2IntMapSerializer() {
			super(false, true);
		}
		
		@Override
		public void write(Kryo kryo, Output output, Long2IntMaps.EmptyMap object) {
		}

		@Override
		public Long2IntMaps.EmptyMap read(Kryo kryo, Input input, Class<Long2IntMaps.EmptyMap> type) {
			return Long2IntMaps.EMPTY_MAP;
		}
	}
	
	public static class SingletonLong2IntMapSerializer extends Serializer<Long2IntMaps.Singleton> {

		public SingletonLong2IntMapSerializer() {
			super(false, true);
		}
		
		@Override
		public void write(Kryo kryo, Output output, Long2IntMaps.Singleton object) {
			assert(object.size() == 0);
			for (Long2IntMap.Entry entry : object.long2IntEntrySet()) {
				output.writeLong(entry.getLongKey());
				output.writeInt(entry.getIntValue());
				break;
			}
		}

		@Override
		public Long2IntMaps.Singleton read(Kryo kryo, Input input, Class<Long2IntMaps.Singleton> type) {
			return (Long2IntMaps.Singleton)Long2IntMaps.singleton(input.readLong(), input.readInt());
		}
	}
}
