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
package edu.usc.goffish.gopher.impl.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class StatLogger {

    private PrintWriter writer;

    private static StatLogger ourInstance = new StatLogger();

    public static StatLogger getInstance() {
        return ourInstance;
    }

    private StatLogger() {

        try {
            writer = new PrintWriter(new FileWriter("bsp-stats.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void log(String msg) {
        System.out.println(msg);
        writer.println(msg);
        writer.flush();
    }


}
