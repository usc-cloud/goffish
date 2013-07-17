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

import junit.framework.TestCase;

import java.util.Arrays;

public class N_Hop_Stat_CollectorTest extends TestCase {

    public void testVantagePointIndexExtract() {

        N_Hop_Stat_Collector collector = new N_Hop_Stat_Collector();

        String[] vantages = new String[]{"3795", "4149", "8443", "6432", "15777"};
        collector.setVantagePoints(Arrays.asList(vantages));

        String []testSample = new String[]{"3795","15775","8443"};

        Integer[] ids = collector.vantageIpIndex(testSample);
        assertTrue(Arrays.asList(ids).contains(0)&&Arrays.asList(ids).contains(2));


    }

}
