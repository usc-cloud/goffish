package com.pfl.dynamicgraphplayer;


import java.awt.Point;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ekimbrel
 */
public class AnimateBuildoutPropertyChangeListener implements PropertyChangeListener{
    
    private JLabel dateLabel;
    private JLabel densityLabel;
    private Histogram promotedHistogram;
    private Histogram valueHistogram;
    private Histogram reentrantHistogram;
    private Histogram firstDegreeTriangleHistogram;
    private Histogram densityHistogram;
    private javax.swing.JSlider _sldrProgress;
    

    

    @Override
    public void propertyChange(PropertyChangeEvent evt) {

            if (evt.getPropertyName().compareTo(AnimateBuildout.events.UPDATE_DATE.toString()) == 0) {
                final String value = String.valueOf(evt.getNewValue());
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        dateLabel.setText(String.valueOf(value));
                    }
                });
            }
        
        

            else if(evt.getPropertyName().compareTo(AnimateBuildout.events.UPDATE_MAX_DENSITY.toString()) ==0)
            {
               final String value = "- Max: " + String.valueOf(evt.getNewValue());
               SwingUtilities.invokeLater(new Runnable() {
                           public void run() {
                               densityLabel.setText(String.valueOf(value));
                           }
                       });

            }

            
           
            else if(evt.getPropertyName().compareTo(AnimateBuildout.events.PROMOTED_NODE.toString())==0)
           {
               final int xC = (Integer) evt.getNewValue();
               
               SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            promotedHistogram.setBand(true);
                            promotedHistogram.addData(xC, 1);                            
                            promotedHistogram.repaint();
                        }
                    });
               

           }
         
            else if(evt.getPropertyName().compareTo("Reentrant Node")==0)
            {
                final String value = String.valueOf(evt.getNewValue());
                SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                //graphDensityLabel.setText(String.valueOf(value));
                            }
                        });

            }
            
            else if (evt.getPropertyName().compareTo(AnimateBuildout.events.TOTAL_VALUE.toString()) == 0 )
            {
                final Point p = (Point) evt.getNewValue(); 
                SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            valueHistogram.setData(p.x, p.y);                            
                            valueHistogram.repaint();
                            promotedHistogram.addData(p.x, 0);
                            promotedHistogram.repaint();
                            reentrantHistogram.addData(p.x, 0);
                            reentrantHistogram.repaint();
                            
                        }
                    });
                
            }
            
            
            else if (evt.getPropertyName().compareTo(AnimateBuildout.events.DATE_SPAN.toString()) == 0 )
            {
                final int fullSpan = (Integer) evt.getNewValue();
                SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            _sldrProgress.setMaximum(fullSpan);
                            _sldrProgress.setValue(0);
                            firstDegreeTriangleHistogram.setData(1, 0);
                            firstDegreeTriangleHistogram.setData((int)fullSpan, 0);
                            valueHistogram.setData(1, 0);
                            valueHistogram.setData((int)fullSpan, 0);
                            densityHistogram.setData(1, 0);
                            densityHistogram.setData((int)fullSpan, 0);
                            promotedHistogram.addData(1, 0);
                            promotedHistogram.addData((int)fullSpan, 0);
                            reentrantHistogram.addData(1, 0);
                            reentrantHistogram.addData((int)fullSpan, 0);
                        }
                });
                        
            }
            
            
            else if (evt.getPropertyName().compareTo(AnimateBuildout.events.UPDATE_TRIANGLES.toString()) == 0 ){
                 final Point p = (Point) evt.getNewValue();
                 SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            firstDegreeTriangleHistogram.setData(p.x, p.y);                            
                            firstDegreeTriangleHistogram.repaint();
                        }
                    });
            }
            
            
            else if (evt.getPropertyName().compareTo(AnimateBuildout.events.UPDATE_DENSITY.toString()) == 0 )
            {

                final Point p = (Point) evt.getNewValue();
                SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            densityHistogram.setBand(true);
                            densityHistogram.setData(p.x, p.y);                            
                            densityHistogram.repaint();
                        }
                    });
            }
            
            
            else if(evt.getPropertyName().compareTo(AnimateBuildout.events.REENTRANT_NODE.toString()) == 0){
                
                final Point p = (Point) evt.getNewValue(); 
                SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            reentrantHistogram.setBand(true);
                            reentrantHistogram.addData(p.x, p.y);                            
                            reentrantHistogram.repaint();
                        }
                    });
            }
            
            else if(evt.getPropertyName().compareTo(AnimateBuildout.events.DATE_PROGRESS.toString()) == 0){
                final int value = (Integer) evt.getNewValue();
                SwingUtilities.invokeLater(new Runnable() {
                   public void run() {
                       _sldrProgress.setValue(value);
                   } 
                });
                
            }
            
            
                        
    }

    /**
     * @param statusLabel the statusLabel to set
     */
    public void setDateLabel(JLabel statusLabel) {
        this.dateLabel = statusLabel;
    }

    /**
     * @param densityLabel the densityLabel to set
     */
    public void setDensityLabel(JLabel densityLabel) {
        this.densityLabel = densityLabel;
    }

    /**
     * @param promotedHistogram the promotedHistogram to set
     */
    public void setPromotedHistogram(Histogram promotedHistogram) {
        this.promotedHistogram = promotedHistogram;
    }

    /**
     * @param valueHistogram the valueHistogram to set
     */
    public void setValueHistogram(Histogram valueHistogram) {
        this.valueHistogram = valueHistogram;
    }

    /**
     * @param reentrantHistogram the reentrantHistogram to set
     */
    public void setReentrantHistogram(Histogram reentrantHistogram) {
        this.reentrantHistogram = reentrantHistogram;
    }

    /**
     * @param firstDegreeTriangleHistogram the firstDegreeTriangleHistogram to set
     */
    public void setFirstDegreeTriangleHistogram(Histogram firstDegreeTriangleHistogram) {
        this.firstDegreeTriangleHistogram = firstDegreeTriangleHistogram;
    }

    /**
     * @param densityHistogram the densityHistogram to set
     */
    public void setDensityHistogram(Histogram densityHistogram) {
        this.densityHistogram = densityHistogram;
    }

    /**
     * @param sldrProgress the _sldrProgress to set
     */
    public void setSldrProgress(javax.swing.JSlider sldrProgress) {
        this._sldrProgress = sldrProgress;
    }
        
    
    
}
