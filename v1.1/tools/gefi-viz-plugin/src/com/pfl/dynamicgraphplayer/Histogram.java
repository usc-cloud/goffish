/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * Histogram.java
 *
 * Created on Mar 27, 2012, 5:02:08 PM
 */
package com.pfl.dynamicgraphplayer;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.util.Date;
import java.util.TreeMap;

/**
 *
 * @author jlarson
 */
public class Histogram extends javax.swing.JPanel {

    public Date minDate;
    public Date maxDate;
    //public int width;
    public int height;
    private TreeMap<Integer, Integer> dataList;
    private TreeMap<Integer, Integer> graphicalBins;
    public double maxValue;
    private Color _clr;
    private boolean highlightOn;
    private int highlightPosition;
    long ymax;
    private boolean hasNegative;
    private String maxTextOn;
    private int age_out;
    private int lastxcoord;
    private boolean showband;
    /** Creates new form Histogram */
    public Histogram() {
        initComponents();
        dataList = new TreeMap<Integer, Integer>();
        maxValue = 0;
        highlightOn = false;
        highlightPosition=0;
        _clr = Color.DARK_GRAY;
        hasNegative=false;
        maxTextOn = "";
        age_out = 90;
        lastxcoord = 0;
        showband = false;
    }
    public void setBand(boolean band)
    {
        showband = band;
    }
    public void setAgeOut(int age)
    {
        age_out = age;
    }
    
    public void setMaxTextOn(String value)
    {
        maxTextOn = value;
    }
    
    public void setColor(Color clr)
    {
        _clr = clr;
    }
    
    public Color getColor()
    {
        return _clr;
    }

    @Override
    public void paint(Graphics g) {
        super.paint(g);
        if(this.getHistogramWidth()==0)
        {
            return;
        }
        
        //Determine the scalar based on the max height
        //This doesn't work because the binning may make it larger!
        //double yscalar = maxValue / (double)this.getHeight();
        //Find out how many bins to walk over when drawing
        int binstowalk = (int)(Math.ceil(getHistogramWidth() / (this.getWidth())))+1;
        
        BufferedImage image = new BufferedImage(this.getHistogramWidth()/(binstowalk), this.getHeight(), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2 = image.createGraphics();
        
        if(lastxcoord > 0 && showband)
        {
            int rightcoord = 0;
            if(lastxcoord-age_out > 0)
            {
                rightcoord = lastxcoord-age_out;
            }
        
            int xwindowbinright = lastxcoord / (binstowalk);
            int xwindowbinleft = (lastxcoord-age_out) / (binstowalk);
            g2.setColor(new Color(20, 200, 255, 31));
            g2.fillRect(xwindowbinleft, 0, xwindowbinright-xwindowbinleft, this.getHeight());
            g2.setColor(Color.BLUE);
            //g2.drawLine(xwindowbinright, 0, xwindowbinright, this.getHeight());
            //g2.drawLine(xwindowbinleft, 0, xwindowbinleft, this.getHeight());
            
        }
        
        //int binstowalk = (int)(Math.ceil(getHistogramWidth() / (this.getWidth())))+1;
        
        if(!hasNegative)
        {
            for (int horiz = 0; horiz < this.getWidth(); horiz++)
            {
                if(graphicalBins != null)
                {
                    if (graphicalBins.containsKey(horiz))
                    {
                        g2.setColor(_clr);

                        g2.fillRect(horiz, this.getHeight()-graphicalBins.get(horiz), 1, graphicalBins.get(horiz));


                        if( highlightPosition/binstowalk == horiz)
                        {
                            g2.setColor(Color.red);
                            g2.fillRect(horiz-1, this.getHeight()-(int)graphicalBins.get(horiz)-1, 1, 1);
                        }
                    }
                }
            }
        }
        else
        {
            //Rendering mid-axis
            for (int horiz = 0; horiz < this.getWidth(); horiz++)
            {
                if(graphicalBins != null)
                {
                    if (graphicalBins.containsKey(horiz))
                    {
                        if(graphicalBins.get(horiz) >=0)
                        {
                            g2.setColor(Color.GREEN);

                            g2.fillRect(horiz, this.getHeight()/2-graphicalBins.get(horiz)/2, 1, graphicalBins.get(horiz)/2);
                        }
                        else
                        {
                            g2.setColor(Color.RED);

                            g2.fillRect(horiz, this.getHeight()/2, 1, Math.abs(graphicalBins.get(horiz))/2);
                        }

                        
                        if( highlightPosition/binstowalk == horiz)
                        {
                            g2.setColor(Color.red);
                            g2.fillRect(horiz-1, this.getHeight()/2-(int)graphicalBins.get(horiz)/2-1, 1, 1);
                        }
                    }
                    
                    
                }
            }
        }
        
        if(!maxTextOn.isEmpty())
        {
            g2.drawString(maxTextOn, 10, 10);
        }
        
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        
        g.drawImage(image.getScaledInstance(this.getWidth(), this.getHeight(), Image.SCALE_DEFAULT), 0, 0, this.getWidth(), this.getHeight(), this);
    }

    private void translateGraphicalBins()
    {
        int binstowalk = (int)(Math.ceil(getHistogramWidth() / (this.getWidth())))+1;
        ymax = 0;
        graphicalBins = new TreeMap<Integer, Integer>();
        
        //First find the max height based on our binning strategy
        for (int horiz = 0; horiz < this.getWidth(); horiz++) {
            height = 0;
            for(int walker = 0; walker < binstowalk; walker ++)
            {
                if(dataList.containsKey(walker+horiz*binstowalk))
                {
                    height += dataList.get(walker+horiz*binstowalk);
                }
            }
            if (Math.abs(height) > ymax)
            {
                ymax = Math.abs(height);
            }
        }
        
        double yscalar = ymax / (double)this.getHeight();
        
        //Next, scale everything down appropriately
        for (int horiz = 0; horiz < this.getWidth(); horiz++) {
            height = 0;
            for(int walker = 0; walker < binstowalk; walker ++)
            {
                if(dataList.containsKey(walker+horiz*binstowalk))
                {
                    height += dataList.get(walker+horiz*binstowalk);
                }
            }
            graphicalBins.put(horiz, (int)(height/yscalar));
        }
        
    }
    
    public void clear() {
        dataList = new TreeMap<Integer, Integer>();
        maxValue = 0;
        highlightOn = false;
        highlightPosition=0;
        _clr = Color.DARK_GRAY;
        hasNegative=false;
        maxTextOn = "";
    }

    public void setHighlightPosition(int pos)
    {
        highlightPosition = pos;
    }
    private int getHistogramWidth() {
        if (dataList != null) {
            if (dataList.size() > 1) {
                //Get the total width
                int histogramWidth = dataList.lastKey() - dataList.firstKey();
                return histogramWidth;
            }
        }
        return 0;
    }

    public void addData(int xCoord, int amount) {
        lastxcoord = xCoord;
        if (dataList.get(xCoord) != null) {
            dataList.put(xCoord, amount + dataList.get(xCoord));
        } else {
            dataList.put(xCoord, amount);
        }
        
        //Update our max value for scaling
        if (Math.abs(dataList.get(xCoord)) > maxValue)
        {
            maxValue = Math.abs(dataList.get(xCoord));
        }
        translateGraphicalBins();
        
        if(amount < 0)
        {
            hasNegative=true;
        }
    }
    
    
    public void setData(int x, int y){
        lastxcoord = x;
        dataList.put(x, y);
        
        //Update our max value for scaling
        if (Math.abs(y) > maxValue)
        {
            maxValue = Math.abs(y);
        }
        translateGraphicalBins();
        
        if(y < 0)
        {
            hasNegative=true;
        }
        
        
    }

    /** This method is called from within the constructor to
     * initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        addComponentListener(new java.awt.event.ComponentAdapter() {
            public void componentResized(java.awt.event.ComponentEvent evt) {
                formComponentResized(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    private void formComponentResized(java.awt.event.ComponentEvent evt) {//GEN-FIRST:event_formComponentResized
        // TODO add your handling code here:
        translateGraphicalBins();
    }//GEN-LAST:event_formComponentResized

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
}
