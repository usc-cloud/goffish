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

import ar.Aggregates;
import ar.ext.server.ARCombiner;
import ar.renderers.ParallelSpatial;
import ar.rules.AggregateReducers;
import ar.rules.Transfers;
import ar.util.Util;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.logging.Logger;

public class SimpleSever {

    private static final Logger logger = Logger.getLogger(SimpleSever.class.getName());


    public static void main(String[] args) throws IOException, InterruptedException {


        String host = "localhost";
        int port = 8739;
        if(args.length == 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        ARCombiner<Integer> c = new ARCombiner<Integer>(host, port,
                new ar.ext.avro.Converters.ToCount(), new AggregateReducers.Count());
        c.start();

        DynamicImageUpdateJFrame dynamicImageUpdateJFrame = null;
        while (true) {

            Aggregates aggregates = c.combined();

            if (aggregates != null) {
                ar.Renderer render = new ParallelSpatial();
                Transfers.Interpolate transfer = new Transfers.Interpolate(new Color(255, 0, 0, 25),
                        new Color(255, 0, 0, 255));

                int width = 600;
                int height = 600;
                Aggregates colors = render.transfer(aggregates, transfer);

               logger.info("Trasnformed the agrrigate " + colors);

                if (dynamicImageUpdateJFrame == null) {
                    BufferedImage cat = Util.asImage(colors, width, height, Color.BLACK);
                    dynamicImageUpdateJFrame = new DynamicImageUpdateJFrame(cat, colors, width, height);
                    dynamicImageUpdateJFrame.setBackground(Color.BLACK);
                    dynamicImageUpdateJFrame.setSize(width+10,height+10);
                    //dynamicImageUpdateJFrame.setVisible(false);
                    dynamicImageUpdateJFrame.setVisible(true);
                    dynamicImageUpdateJFrame.render();
                } else {
                    dynamicImageUpdateJFrame.updateImage(colors);
                    dynamicImageUpdateJFrame.render();
                   // dynamicImageUpdateJFrame.setVisible(false);
                    dynamicImageUpdateJFrame.setVisible(true);
                }
            }

            Thread.sleep(1000);
        }


    }
}
