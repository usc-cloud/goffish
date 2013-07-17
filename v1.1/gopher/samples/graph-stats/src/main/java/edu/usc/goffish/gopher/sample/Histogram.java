/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.goffish.gopher.sample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/***
 * Keep track of histogram statistics
 * 
 * @author simmhan
 *
 */
public class Histogram {
	private Map<Long, Long> histogramMap;
	private boolean isDefaultAddToMinBucket;
	private List<Long> bucketBoundaries;
	private long minValue = Long.MAX_VALUE;
	private String minID;
	private long maxValue = Long.MIN_VALUE;
	private String maxID;
	private long count = 0;
	private long sum = 0;
	private boolean active = false;
	private final String NULL = "NULL";
	
	public Histogram(List<Long> bucketBoundaries_, boolean defaultAddToMinBucket_){
		
		bucketBoundaries = new ArrayList<Long>(bucketBoundaries_); // copy to local
    	// the bucketBoundary is reverse sorted, histogram has been initialized
    	Collections.sort(bucketBoundaries);
    	Collections.reverse(bucketBoundaries);

		histogramMap = new HashMap<Long, Long>(bucketBoundaries.size());
    	
    	// assert: the bucketBoundary is reverse sorted
    	for(Long boundary : bucketBoundaries){
    		histogramMap.put(boundary, 0L);
    	}    		
    	
    	isDefaultAddToMinBucket = defaultAddToMinBucket_;
	}

	/***
	 * Pass input as output from Histogram.saveToString();
	 * 
	 * @param serializedSource
	 */
	public Histogram(String serializedSource){
		histogramMap = new HashMap<Long, Long>();
		loadFromString(serializedSource);
	}
	
	
	public boolean add(long value){
		return add(value, NULL);
	}
	
	/***
	 * 
	 * @param value
	 * @param histogramMap
	 * @return return true if it was added to a bucket (even if default bucket). False if not added.
	 */
    public boolean add(long value, String id){
    	
    	Long lastBoundary = Long.MIN_VALUE;
    	for(Long boundary : bucketBoundaries){
    		lastBoundary = boundary;
    		if(value >= boundary) {
    			Long prevValue = histogramMap.get(boundary);
    			assert prevValue != null;
    			histogramMap.put(boundary, prevValue + 1);

    			if(value < minValue) { minValue = value; minID = id;}
    			if(value > maxValue) { maxValue = value; maxID = id;}
    			count++;
    			sum+=value;
    			active = true;
    			return true;
    		}
    	}
    	
		// by default, add to smallest bucket (i.e. last bucket in the reverse bucket list
    	if(isDefaultAddToMinBucket) {
			Long prevValue = histogramMap.get(lastBoundary);
			histogramMap.put(lastBoundary, prevValue + 1);

			if(value < minValue) { value = minValue; minID = id;}
			if(value > maxValue) { value = maxValue; maxID = id;}
			count++;
			sum+=value;
			active = true;
			return true;
    	}
    	
    	return false;
    }

    /***
     * Merges another histogram values with the curent histogram
     * 
     * @param otherHistogram
     * @return
     */
    public boolean merge(Histogram otherHistogram){
    	if(otherHistogram == null || otherHistogram.isDefaultAddToMinBucket != this.isDefaultAddToMinBucket)
    		return false;
    	
    	if(!otherHistogram.active) return true;
    	
    	if(otherHistogram.active) {
    		
    		if(otherHistogram.bucketBoundaries == null || otherHistogram.histogramMap == null ||
    		this.bucketBoundaries.size() != otherHistogram.bucketBoundaries.size()){
    			return false;
    		}
    		
    		for(int i=0; i<this.bucketBoundaries.size(); i++){
    			if(this.bucketBoundaries.get(i) != otherHistogram.bucketBoundaries.get(i)) return false;
    		}
    		
    		this.active = true;
    		
    		// increment frequency of histogram
    		for(Entry<Long, Long> kvp : otherHistogram.histogramMap.entrySet()) {
    			long key = kvp.getKey();
    			long value = this.histogramMap.get(key);
    			value += kvp.getValue();
    			this.histogramMap.put(key, value);
    		}
    		
    		// update count/sum/min/max values
    		this.count += otherHistogram.count;
    		this.sum += otherHistogram.sum;
    		if(otherHistogram.minValue < this.minValue) {
    			this.minValue = otherHistogram.minValue;
    			this.minID = otherHistogram.minID;    					
    		}
    		if(otherHistogram.maxValue > this.maxValue) {
    			this.maxValue = otherHistogram.maxValue;
    			this.maxID = otherHistogram.maxID;    					
    		}    		
    	}
    	
    	return true;
    }

    
    public String toString(){
    	return saveToString();
    }
    
    public String toPrettyString(){
    	return prettyPrint(new StringBuffer()).toString();
    }
    
    public StringBuffer prettyPrint(StringBuffer sb){
    	if(active) {
    		sb.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        	sb.append("Boundary\t# of Values > Boundary\n");
        	sb.append("--------\t----------=====-------\n");
		List<Long> buckets = new ArrayList<Long>(histogramMap.keySet());
        	Collections.sort(buckets);        	
        	for(Long key : buckets){
        		sb.append(key).append('\t').append(histogramMap.get(key)).append('\n');
        	}
    		sb.append("========\t======================\n");
        	if(isDefaultAddToMinBucket) 
        		sb.append("(NOTE: Min bucket includes frequency of default values outside boundaries)\n");
        	sb.append("Total Count of Values").append('\t').append(count).append('\n');
        	sb.append("Sum of Values").append('\t').append(sum).append('\n');
        	sb.append("Mean of Values").append('\t').append(sum/count).append('\n');
        	sb.append("Min Value/ID").append('\t').append(minValue).append('\t').append(minID).append('\n');
        	sb.append("Max Value/ID").append('\t').append(maxValue).append('\t').append(maxID).append('\n');
    		sb.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        	sb.append("\n");
    	} else {
    		sb.append("No active values were added to histogram. Empty!\n\n");
    	}

    	return sb;
    }
        
    
    public String saveToString() {
    	StringBuffer sb = new StringBuffer();
    	for(Entry<Long, Long> kvp : histogramMap.entrySet()){
    		sb.append(kvp.getKey()).append(',').append(kvp.getValue()).append(',');
    	}
    	
    	sb.append(';').append("count").append(';').append(count);
    	sb.append(';').append("sum").append(';').append(sum);

    	assert minID.indexOf(',') < 0;
    	assert minID.indexOf(';') < 0;
    	sb.append(';').append("min").append(';').append(minValue).append(';').append(minID);
  
    	assert maxID.indexOf(',') < 0;
    	assert maxID.indexOf(';') < 0;
    	sb.append(';').append("max").append(';').append(maxValue).append(';').append(maxID);
    	
    	sb.append(';').append("active").append(';').append(active);
    	sb.append(';').append("useDefault").append(';').append(isDefaultAddToMinBucket);
    	
    	return sb.toString();
    }
    
    private void loadFromString(String source) {

		String[] splits = source.split(",");
    	bucketBoundaries = new ArrayList<Long>();
    	
    	int i;
    	for(i=0; i<splits.length; i++){
    		// stop if last value
    		if(splits[i].startsWith(";")) break;

    		// load the boundary values
    		long bound = Long.parseLong(splits[i]);
    		bucketBoundaries.add(bound);
    		i++;
        	// load the histogram values
    		histogramMap.put(bound, Long.parseLong(splits[i]));        		
    	}
    	
    	// the bucketBoundary is reverse sorted, histogram has been initialized
    	Collections.sort(bucketBoundaries);
    	Collections.reverse(bucketBoundaries);
    	
    	assert splits[i].startsWith(";");
    	splits = splits[i].split(";");
    	
    	// 0: ""
    	// 1,2: count
    	// 3,4: sum
    	// 5,6,7: min val, id
    	// 8,9,10: max val, id
    	// 11,12: active
    	// 13,14: default
    	i=0;
    	assert "".equals(splits[i]);
    	
    	i++;        	
    	assert "count".equals(splits[i]);
    	i++;
    	count = Long.parseLong(splits[i]);
    	
    	i++;        	
    	assert "sum".equals(splits[i]);
    	i++;
    	sum = Long.parseLong(splits[i]);

    	i++;
    	assert "min".equals(splits[i]);
    	i++;
    	minValue = Long.parseLong(splits[i]);
    	i++;
    	minID = splits[i];
    	
    	i++;
    	assert "max".equals(splits[i]);
    	i++;
    	maxValue = Long.parseLong(splits[i]);
    	i++;
    	maxID = splits[i];
    	
    	i++;
    	assert "active".equals(splits[i]);
    	i++;
    	active = Boolean.parseBoolean(splits[i]);
    	
    	i++;
    	assert "useDefault".equals(splits[i]);
    	i++;
    	isDefaultAddToMinBucket = Boolean.parseBoolean(splits[i]);        	
    }
    
	public Map<Long, Long> getHistogram() {
		return histogramMap;
	}

	public long getMinValue() {
		return minValue;
	}

	public long getMaxValue() {
		return maxValue;
	}

	public long getCount() {
		return count;
	}

	public long getSum() {
		return sum;
	}
}
