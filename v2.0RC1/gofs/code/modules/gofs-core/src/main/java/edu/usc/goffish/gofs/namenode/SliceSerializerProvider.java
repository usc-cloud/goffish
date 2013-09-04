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

import java.lang.reflect.*;

import edu.usc.goffish.gofs.slice.*;

public final class SliceSerializerProvider {

	private SliceSerializerProvider() {
		throw new UnsupportedOperationException();
	}
	
	@SuppressWarnings("unchecked")
	public static Class<? extends ISliceSerializer> loadSliceSerializerType(String typeName) throws ClassNotFoundException, ReflectiveOperationException {
		Class<?> givenType = Class.forName(typeName);
		if (!ISliceSerializer.class.isAssignableFrom(givenType) || givenType.isInterface() || Modifier.isAbstract(givenType.getModifiers())) {
			throw new IllegalArgumentException(typeName + " is not a concrete class deriving from ISliceSerializer");
		}
		
		return (Class<? extends ISliceSerializer>)givenType;
	}
	
	public static ISliceSerializer loadSliceSerializer(String typeName) throws ClassNotFoundException, ReflectiveOperationException {
		return loadSliceSerializer(loadSliceSerializerType(typeName));
	}
	
	public static ISliceSerializer loadSliceSerializer(Class<? extends ISliceSerializer> type) throws ReflectiveOperationException {
		Constructor<? extends ISliceSerializer> sliceSerializerConstructor = type.getConstructor();
		return sliceSerializerConstructor.newInstance();
	}
}
