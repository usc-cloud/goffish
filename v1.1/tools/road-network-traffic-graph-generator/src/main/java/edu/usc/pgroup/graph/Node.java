/*
 * Copyright 2011, University of Southern California. All Rights Reserved.
 * 
 * This software is experimental in nature and is provided on an AS-IS basis only. 
 * The University SPECIFICALLY DISCLAIMS ALL WARRANTIES, EXPRESS AND IMPLIED, INCLUDING WITHOUT 
 * LIMITATION ANY WARRANTY AS TO MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * 
 * This software may be reproduced and used for non-commercial purposes only, 
 * so long as this copyright notice is reproduced with each such copy made.
 */
package edu.usc.pgroup.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Node {


    private int nodeId;

    private List<String> lids = new ArrayList<String>();

    private List<String> colors = new ArrayList<String>();

    private List<String> makeModel = new ArrayList<String>();

    private List<Edge> edges = new ArrayList<Edge>();

    private List<String> lidsForNextIt = new ArrayList<>();

    private List<CarsInBetweenNodes> carsInBetween = new ArrayList<CarsInBetweenNodes>();

    public Node(int nodeId) {
        this.nodeId = nodeId;
    }

    public List<Edge> getEdges() {
        return edges;
    }


    public int getNodeId() {
        return nodeId;
    }

    public List<String> getLids() {
        return lids;
    }

    public List<String> getColors() {
        return colors;
    }

    public List<String> getMakeModel() {
        return makeModel;
    }

    public void setLids(List<String> lids) {
        this.lids = lids;
    }

    public void setCarsInBetween(List<CarsInBetweenNodes> carsInBetween) {
        this.carsInBetween = carsInBetween;
    }


    public void setEdges(List<Edge> edges) {
        this.edges = edges;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Node newNode = new Node(nodeId);

        ArrayList<String> nLid = new ArrayList<String>();
        nLid.addAll(getLids());
        for(String i : getLidsForNextIt()) {
            nLid.add(i);
        }
        newNode.setLids(nLid);

        ArrayList<CarsInBetweenNodes>  nCarsInBetweenNodeses = new ArrayList<CarsInBetweenNodes>();
        for(CarsInBetweenNodes c : carsInBetween) {
            nCarsInBetweenNodeses.add((CarsInBetweenNodes) c.clone());
        }

        newNode.setEdges(edges);
        newNode.setCarsInBetween(nCarsInBetweenNodeses);

        return newNode;
    }

    public void moveCars(int diffInMins, Graph graph) {

        if (this.getEdges().size() <= 0) {
            return;
        }

        int speed = new Random().nextInt(60);
        speed = speed < 5 ? 5 : speed;

        Random r = new Random();

        List<CarsInBetweenNodes> toRemoveInBetween = new ArrayList<CarsInBetweenNodes>();

        for (CarsInBetweenNodes nodes : carsInBetween) {

            int remaining = nodes.getEdge().getDistance() - nodes.getTraveledDistance();
            int d = distanceTraveled(speed, diffInMins);

            if (d >= remaining) {
                toRemoveInBetween.add(nodes);

                graph.getNodes().get(nodes.getEdge().getNode2()).getLidsForNextIt().add(nodes.getLid());

            } else {
                nodes.setTraveledDistance(nodes.getTraveledDistance() + d);
            }

        }


        for (CarsInBetweenNodes c : toRemoveInBetween) {
            carsInBetween.remove(c);

        }

        List<String> toRemove = new ArrayList<String>();
        for (String lid : lids) {
            //get and random edge to move
            //move


            int eId = r.nextInt(this.getEdges().size());

            Edge e = getEdges().get(eId);

            int distenceTrav = distanceTraveled(speed, diffInMins);
            if (distenceTrav >= e.getDistance()) {
                //remove lid from this add to sink
                toRemove.add(lid);
                graph.getNodes().get(e.getNode2()).getLidsForNextIt().add(lid);


            } else {
                CarsInBetweenNodes newCar = new CarsInBetweenNodes(lid, e, distenceTrav);
                carsInBetween.add(newCar);
                toRemove.add(lid);
            }

        }

        for (String lid : toRemove) {
            this.getLids().remove(lid);
        }


    }


    public List<String> getLidsForNextIt() {
        return lidsForNextIt;
    }


    private class CarsInBetweenNodes {
        String lid;
        Edge edge;
        int traveledDistance;

        private CarsInBetweenNodes(String lid, Edge edge, int traveledDistance) {
            this.lid = lid;
            this.edge = edge;
            this.traveledDistance = traveledDistance;
        }

        public String getLid() {
            return lid;
        }

        public Edge getEdge() {
            return edge;
        }

        public int getTraveledDistance() {
            return traveledDistance;
        }

        public void setTraveledDistance(int traveledDistance) {
            this.traveledDistance = traveledDistance;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }



    private static int distanceTraveled(int speedKmph, int diffInmins) {
        double metersMin = ((double) speedKmph * 1000) / ((double) 60);
        return (int) Math.ceil(metersMin * diffInmins);
    }
}
