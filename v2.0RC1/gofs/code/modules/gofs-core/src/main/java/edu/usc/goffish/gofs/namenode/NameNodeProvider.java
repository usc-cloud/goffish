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
import java.net.*;

import org.apache.commons.configuration.*;

public final class NameNodeProvider {
	
	private NameNodeProvider() {
		throw new UnsupportedOperationException();
	}
	
	public static IInternalNameNode loadNameNodeFromConfig(Configuration config, String nameNodeTypeKey, String nameNodeLocationKey) throws ClassNotFoundException, ReflectiveOperationException {
		// retrieve name node type
		Class<? extends IInternalNameNode> nameNodeType;
		{
			String nameNodeTypeString = config.getString(nameNodeTypeKey);
			if (nameNodeTypeString == null) {
				throw new ConversionException("Config must contain key " + nameNodeTypeKey);
			}
			
			try {
				nameNodeType = NameNodeProvider.loadNameNodeType(nameNodeTypeString);
			} catch (ReflectiveOperationException e) {
				throw new ConversionException("Config key " + nameNodeTypeKey + " has invalid format - " + e.getMessage());
			}
		}
		
		// retrieve name node location
		URI nameNodeLocation;
		{
			String nameNodeLocationString = config.getString(nameNodeLocationKey);
			if (nameNodeLocationString == null) {
				throw new ConversionException("Config must contain key " + nameNodeLocationKey);
			}
			
			try {
				nameNodeLocation = new URI(nameNodeLocationString);
			} catch (URISyntaxException e) {
				throw new ConversionException("Config key " + nameNodeLocationKey + " has invalid format - " + e.getMessage());
			}
		}
		
		return loadNameNode(nameNodeType, nameNodeLocation);
	}
	
	@SuppressWarnings("unchecked")
	public static Class<? extends IInternalNameNode> loadNameNodeType(String typeName) throws ClassNotFoundException, ReflectiveOperationException {
		Class<?> givenType = Class.forName(typeName);
		if (!IInternalNameNode.class.isAssignableFrom(givenType) || givenType.isInterface() || Modifier.isAbstract(givenType.getModifiers())) {
			throw new IllegalArgumentException(typeName + " is not a concrete class deriving from IInternalNameNode");
		}
		
		return (Class<? extends IInternalNameNode>)givenType;
	}
	
	public static IInternalNameNode loadNameNode(String typeName, URI location) throws ClassNotFoundException, ReflectiveOperationException {
		return loadNameNode(loadNameNodeType(typeName), location);
	}
	
	public static IInternalNameNode loadNameNode(Class<? extends IInternalNameNode> type, URI location) throws ReflectiveOperationException {
		Constructor<? extends IInternalNameNode> nameNodeConstructor;
		try {
			nameNodeConstructor = type.getConstructor(URI.class);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(type.getName() + " does not have a constructor of the form " + type.getSimpleName() + "(" + URI.class.getName() + ")");
		}
		
		return nameNodeConstructor.newInstance(location);
	}
}
