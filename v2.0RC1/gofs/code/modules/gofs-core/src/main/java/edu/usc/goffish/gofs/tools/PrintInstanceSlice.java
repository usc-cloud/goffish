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
package edu.usc.goffish.gofs.tools;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.slice.*;

public class PrintInstanceSlice {

	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}

		// optional args
		ISliceSerializer serializer = new JavaSliceSerializer();

		// parse optional arguments
		int i;
		OptArgLoop: for (i = 0; i < args.length - REQUIRED_ARGS; i++) {
			switch (args[i]) {
			case "-serializer:format":
				i++;

				switch (args[i]) {
				case "java":
					break;
				case "kryo":
					serializer = new KryoSliceSerializer();
					break;
				default:
					PrintUsageAndQuit(null);
				}

				break;
			default:
				break OptArgLoop;
			}
		}

		// required arguments
		
		Path partitionDirPath = null;
		try {
			partitionDirPath = Paths.get(args[i]);
			i++;
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("slice directory must be valid path - " + e.getMessage());
		}
		
		SliceManager sliceManager = (SliceManager)SliceManager.create(serializer, new FileStorageManager(partitionDirPath));

		while (i < args.length) {

			// parse required arguments
			
			UUID sliceId = null;
			try {
				sliceId = UUID.fromString(args[i]);
			} catch (IllegalArgumentException e) {
				PrintUsageAndQuit("slice uuid must be valid - " + e.getMessage());
			}

			System.out.print("Reading slice " + sliceId + "... ");
			
			long time = System.currentTimeMillis();
			String sliceString = sliceManager.printInstance(sliceId);
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
			
			System.out.println(sliceString);
			
			i++;
		}
	}

	private static final int REQUIRED_ARGS = 2;

	public static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  PrintInstanceSlice [args] <slicedirpath> <sliceuuid1> [<sliceuuid2> ...]");
		System.out.println("  Args: [-serializer:format java|kryo]");
		System.exit(0);
	}
}
