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

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;

public class DynamicImageUpdateJFrame extends JFrame {

    private BufferedImage IMAGE = null;
    private Aggregates<Color> current = null;


    public DynamicImageUpdateJFrame(BufferedImage map, Aggregates<Color> current, int height, int width)
    {
        IMAGE = new BufferedImage(height,width, BufferedImage.TYPE_INT_RGB);
        this.current = current;
        this.IMAGE = map;
     //   render();
    }

    public DynamicImageUpdateJFrame(BufferedImage map) {
        IMAGE = map;
        render();
    }

    public void paint(Graphics g) {
        if (IMAGE == null)
            super.paint(g);
        else
            g.drawImage(IMAGE, 0, 0, this);
    }

    public void render() {

        int cellWidth = 5;
        int cellHeight = 5;

        int imgW = IMAGE.getWidth();
        int imgH = IMAGE.getHeight();
        Graphics2D g2 = IMAGE.createGraphics();
        //g2.clearRect(0,0,DISPLAY_IMAGE.getWidth(),DISPLAY_IMAGE.getHeight());

        for (int x=Math.max(0, current.lowX()); x<Math.min(imgW, current.highX()); x++) {
            for (int y=Math.max(0, current.lowY()); y<Math.min(imgH, current.highY()); y++) {
                Color c = current.at(x, y);
                if (c != null) {IMAGE.setRGB(x, y, c.getRGB());}
            }
        }
        //g2.setColor(Color.black);
        g2.dispose();
        repaint();
        revalidate();
        System.out.println("XX");
    }

    public void updateImage(Aggregates<Color> aggs) {
        this.current = aggs;
    }
}
