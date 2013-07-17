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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Graph {


    private int id;

    private List<Edge> edges = new ArrayList<Edge>();

    private Map<Integer,Node> nodes = new HashMap<Integer,Node>();

    private long startTime;

    private long endTime;

    public Graph() {

    }

    public Graph(int id,long startTime,long endTime) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void addEdge(Edge e) {
        edges.add(e);
    }

    public void addNode(int id,Node n) {
        nodes.put(id,n);
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public Map<Integer, Node> getNodes() {
        return nodes;
    }

    public int getId() {
        return id;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setEdges(List<Edge> edges) {
        this.edges = edges;
    }

    public void setNodes(Map<Integer, Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Graph graph = new Graph();

        graph.setId(graph.getId());
        graph.setStartTime(graph.getStartTime());
        graph.setEndTime(graph.getEndTime());

        Map<Integer, Node> newNodes = new HashMap<Integer, Node>();
        for(Node node : nodes.values()) {
            newNodes.put(node.getNodeId(), (Node) node.clone());
        }

        graph.setNodes(newNodes);
        graph.setEdges(edges);

        return graph;
    }
}
