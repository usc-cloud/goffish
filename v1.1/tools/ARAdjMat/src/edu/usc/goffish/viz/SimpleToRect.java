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
package edu.usc.goffish.viz;

import ar.glyphsets.implicitgeometry.Shaper;

import java.awt.geom.Rectangle2D;

public class SimpleToRect implements Shaper<String> {

    private int maxId;
    private int minId;

    private double width,height;


    public SimpleToRect(double width,double height,int minId,int maxId) {
        this.width = width;
        this.height = height;
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public Rectangle2D shape(String cell) {

        double x = 0;
        double y = 0;

        String []parts = cell.split(":");
        long source  = Long.parseLong(parts[0]);

        String[]vals = parts[1].split("#");

        long     sink = Long.parseLong(vals[1]);

        y = source;
        x = sink;


        return new Rectangle2D.Double(x,y,width,height);
    }
}
