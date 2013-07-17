/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package edu.usc.pgroup.goffish.gopher.sample;



import edu.usc.goffish.gofs.TemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class VectorComparison extends GopherSubGraph {
	private String myStr;
	private List<Integer> myVecIndex;
	private List<double[]> myVec;
    
	@Override
    public void compute(List<SubGraphMessage> subGraphMessages) {
		if(getSuperStep() == 0) {
			extractMyVectors();
			
			// 0 Super-Step : Calculate the intra-subgraph vector comparisons
			int length = myVecIndex.size();
	        
			try {
	            File file = new File("vector-comparison" + partition.getId() + "-" + getSuperStep() + ".txt");
	            PrintWriter writer = new PrintWriter(file);
	                
	            double result = 0;
	            for(int i = 0; i < length; ++i) {
					for(int j = i + 1; j < length; ++j) {
						double[] a = myVec.get(i);
						double[] b = myVec.get(j);
						result = pCorr(a, b);	
						writer.print(myVecIndex.get(i) + "," + myVecIndex.get(j) + "\t" + result + "\n");
					}
				}
	            
	            writer.flush();
	            writer.close();
            } catch (FileNotFoundException e) {
	            e.printStackTrace();
            }
			
			// Send myStr to the next partition
			SubGraphMessage<String> msg = new SubGraphMessage<String>(myStr.getBytes());
         	long nextIndex = (partition.getId() + 1) % 4;
            sentMessage(nextIndex, msg);
		} else {		
			List<Integer> vecIndex = new ArrayList<Integer> ();
			List<double[]> vec = new ArrayList<double[]> ();
			
			// Get message from the predecessor
			String reStr = new String(subGraphMessages.get(0).getData());
			
			// Convert the received string to the vector format
            extractVectors(reStr, vecIndex, vec);
            
			// Start to do the calculation
			int myLength = myVecIndex.size();
            int length = vecIndex.size();
    			
			try {
                File file = new File("vector-comparison" + partition.getId() + "-" + getSuperStep() + ".txt");
                PrintWriter writer = new PrintWriter(file);
                
                double result = 0;
                
                for(int i = 0; i < myLength; ++i) {
					for(int j = 0; j < length; ++j) {
						double[] a = myVec.get(i);
						double[] b = vec.get(j);
						
						if(a.length == b.length) {
							result = pCorr(a, b);	
							writer.print(myVecIndex.get(i) + "," + vecIndex.get(j) + "\t" + result + "\n");
						}
					}
				}
                
                writer.flush();
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
    		 
            if(getSuperStep() == 3) {
                voteToHalt();
            } else {
            	SubGraphMessage<String> msg = new SubGraphMessage<String>(reStr.getBytes());
             	long nextIndex = (partition.getId() + 1) % 4;
                sentMessage(nextIndex, msg);
            } 
        }
    }
	
    // Utility 
	private void extractMyVectors() {
		myStr = "";
		myVecIndex = new ArrayList<Integer> ();
    	myVec = new ArrayList<double[]> ();
    	
    	// Put vertex properties into myStr
    	Iterable<TemplateVertex> it = (Iterable<TemplateVertex>) subgraph.getTemplate().vertices();
    	
    	String value;
    	long vInd = 0;
    	for(TemplateVertex v: it) {
    		vInd = v.getId();
    		if(!subgraph.isRemoteVertex(vInd)) {
        		value = (String) subgraph.getVertexProperties().getProperty("v1").getDefaults().get(v.getId());
        		myStr += (value + "X");
            }
    	}
    	
    	extractVectors(myStr, myVecIndex, myVec);
	}
	
    // Utility
	private void extractVectors(String str, List<Integer> vecIndexList, List<double[]> vecEntryList) {
		String[] conStr = str.split("X");
		String[] vectorPair;
		
		String vecIndexInStr = null;
		String vecEntryInStr = null;
		
		for(int i = 0; i < conStr.length; ++i) {			
			vectorPair = conStr[i].split("\t");
			
			try {
				vecIndexInStr = vectorPair[0];
				vecIndexList.add(Integer.parseInt(vecIndexInStr));

				vecEntryInStr = vectorPair[1];
				vecEntryList.add(strToVector(vecEntryInStr));
			} catch (Exception e) {
				System.out.println(vecIndexInStr);
				e.printStackTrace();
			}
		}
	}
	
	// Utility
	private double[] strToVector(String str) {
		String[] entry = str.split(",");
		int length = entry.length;
		double[] reIntArray = new double[length];
		
		for(int i = 0; i < entry.length; ++i) {
			reIntArray[i] = Integer.parseInt(entry[i]);
		}
		
		return reIntArray;
	}

	private double pCorr(double[] x, double[] y) {
		double[] xy = prod(subtractAv(x), subtractAv(y));
		double[] xx = prod(subtractAv(x), subtractAv(x));
		double[] yy = prod(subtractAv(y), subtractAv(y));
		
		double num = sum(xy);
		
		double den = Math.sqrt(sum(xx) * sum(yy));
		
		if(den == 0) {
			return 0;
		}
		
		return num / den;
	} 
	
	private double[] subtractAv(double[] a) {
		int length = a.length;
		double av = av(a);
		
		double[] r = new double[length];
		
		for(int i = 0; i < length; ++i) {
			r[i] = a[i] - av;
		}
		
		
		return r;
	}
	
	private double[] prod(double[] a, double[] b) {
		int length = a.length;
		
		double[] r = new double[length];
		
		for(int i = 0; i < length; ++i) {
			r[i] = a[i] * b[i];
		}
		
		return r;
	}
	
	private double av(double[] a) {
		double sum = sum(a);
		
		return sum / a.length;
	}
	
	private double sum(double[] a) {
		double r = 0;
		
		for(int i = 0; i < a.length; ++i) {
			r += a[i];
		}
		
		return r;
	}
}