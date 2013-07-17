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

import java.io.*;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;


public class GraphGenerator {


    Semaphore ioThreadLock = new Semaphore(1);
    ExecutorService executor = Executors.newFixedThreadPool(5);
    static Properties properties = null;
    static {
        properties = new Properties();
        try {
            properties.load(new FileInputStream("generator.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {

        String count = properties.getProperty("instance-count");

        int c = 1000;

        try {
            c = Integer.parseInt(count);
        } catch (Exception e) {
            c =1000;
        }



        GraphGenerator graphGenerator = new  GraphGenerator();
        System.out.println("Start creating template");
        Graph g = graphGenerator.createTemplate();
        System.out.println("Template Created ...");
        graphGenerator.writeGraphTemplate(g);
        System.out.println("Write Template done");
        long startTime = System.currentTimeMillis();
        Graph newGrap = (Graph) g.clone();
        for(int i=0;i<c;i++) {

            for(Node n : newGrap.getNodes().values()) {
                n.moveCars(5,newGrap);
            }
            newGrap = (Graph) newGrap.clone();
            newGrap.setId(i+1);
            newGrap.setStartTime(startTime);
            newGrap.setEndTime(startTime + 1000*5*60);

            graphGenerator.writeGraphInstance(newGrap);
            newGrap = (Graph) newGrap.clone();
            startTime  +=  1000*5*60;
        }

    }


    public Graph createTemplate() throws Exception {
        Graph graph = new Graph();


        BufferedReader reader = new BufferedReader(new FileReader(properties.getProperty("topology-graph")));

        String line;
        int eId = 1;

        Random random = new Random();

        while ((line = reader.readLine()) != null) {
            if (line.charAt(0) != '#') {

                String[] nodes = line.split("\t");
                if(nodes.length == 1) {
                    nodes = line.split(" ");
                }
                int n1 = Integer.parseInt(nodes[0]);
                int n2 = Integer.parseInt(nodes[1]);
                n1 +=1;
                n2 +=1;
                if(!graph.getNodes().containsKey(n1)) {
                  Node node = new Node(n1);
                  String id = "" + n1 + "abc";
                  node.getLids().add(id);
                  graph.addNode(n1,node);
                }

                if(!graph.getNodes().containsKey(n2)) {
                    Node node = new Node(n2);
                    String id = "" + n2 + "abc";
                    node.getLids().add(id);
                    graph.addNode(n2,node);
                }

                Edge e = new Edge(eId++,n1,n2);
                int i;
                int d  =  (i=random.nextInt(400)) < 50? 50 : i;
                e.setDistance(d);
                graph.addEdge(e);
                
                
                graph.getNodes().get(n1).getEdges().add(e);

            }
        }

        return graph;
    }


    private void writeWithNewLine(PrintWriter writer,String line) {
        writer.println(line);
    }

    private void write(PrintWriter writer,String line) {
        writer.write(line);
    }



    private void writeGraphTemplate(Graph graph) {

        TemplateWriter writer = new TemplateWriter(graph);
        try {
            ioThreadLock.acquire();
        } catch (InterruptedException e) {

        }
        executor.execute(writer);

    }

    private void writeGraphInstance(Graph graph) {
        InstanceWriter writer = new InstanceWriter(graph);
        try {
            ioThreadLock.acquire();
        } catch (InterruptedException e) {

        }
        executor.execute(writer);
    }



    private class TemplateWriter implements Runnable {
        Graph g;

        public TemplateWriter(Graph g) {
            this.g = g;
        }

        @Override
        public void run() {
            try {

                System.out.println("Start writing template");
                PrintWriter fwrite = new PrintWriter(new BufferedOutputStream(
                        new FileOutputStream(new File("" + properties.getProperty("dir") + "/" +
                                properties.getProperty("prefix") + "template.gml"))));



                writeWithNewLine(fwrite, "graph [");
                writeWithNewLine(fwrite, "\tdirected 1");
                writeWithNewLine(fwrite, "\tvertex_properties [");
                writeWithNewLine(fwrite, "\t\tlicense_list [");
                writeWithNewLine(fwrite, "\t\t\tis_static 0");
                writeWithNewLine(fwrite, "\t\t\ttype \"string\"");
                writeWithNewLine(fwrite, "\t\t]");
                writeWithNewLine(fwrite, "\t]");
                writeWithNewLine(fwrite, "\tedge_properties [");
                writeWithNewLine(fwrite, "\t\tdist [");
                writeWithNewLine(fwrite, "\t\t\tis_static 0");
                writeWithNewLine(fwrite, "\t\t\ttype \"integer\"");
                writeWithNewLine(fwrite, "\t\t]");
                writeWithNewLine(fwrite, "\t]");
                System.out.println("Finished template header writing ");

                //Nodes
                for(Node n : g.getNodes().values()) {
                    writeWithNewLine(fwrite, "node [");
                    writeWithNewLine(fwrite, "id " + n.getNodeId());
                    writeWithNewLine(fwrite, "license_list " + "\"" + n.getLids().get(0) + "\"");
                    writeWithNewLine(fwrite, "]");
                }


                System.out.println("Finished template nodes");
                //Edges
                for(Edge e : g.getEdges()) {
                    writeWithNewLine(fwrite, "edge [");
                    writeWithNewLine(fwrite, "id " + e.getEdgeId());
                    writeWithNewLine(fwrite, "source " + e.getNode1());
                    writeWithNewLine(fwrite, "target " + e.getNode2()) ;
                    writeWithNewLine(fwrite, "dist " +  e.getDistance());
                    writeWithNewLine(fwrite, "]");
                }
                System.out.println("Finished template Edges");
                write(fwrite, "]");
                fwrite.flush();
                fwrite.close();
                System.out.println("Template done");
                ioThreadLock.release();;

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


    private class InstanceWriter implements Runnable {

        Graph graph;

        public InstanceWriter(Graph graph) {
            this.graph = graph;
        }

        @Override
        public void run() {

            try {

                System.out.println("Start writing instance ");
                PrintWriter fwrite = new PrintWriter(new BufferedOutputStream(
                        new FileOutputStream(new File("" + properties.getProperty("dir") + "/" +
                                properties.getProperty("prefix") +"instance_"+ graph.getId()+".gml"))));

                writeWithNewLine(fwrite, "graph [");
                writeWithNewLine(fwrite, "\tid " + graph.getId());
                writeWithNewLine(fwrite, "\ttimestamp_start " + graph.getStartTime());
                writeWithNewLine(fwrite, "\ttimestamp_end " + graph.getEndTime());

                System.out.println("Writing nodes ");

                for(Node node: graph.getNodes().values()) {
                    writeWithNewLine(fwrite, "node [");
                    writeWithNewLine(fwrite, "id " + node.getNodeId());

                    String idList = "";

                    for(int i=0;i< node.getLids().size();i++) {
                        if(i != (node.getLids().size() -1)){
                            idList += node.getLids().get(i) + ",";
                        } else {
                            idList += node.getLids().get(i);
                        }
                    }

                    writeWithNewLine(fwrite, "license_list " + "\"" + ("".equals(idList)?"empty":idList) + "\"");
                    writeWithNewLine(fwrite, "]");

                }
                System.out.println("Writing edges..");

                for(Edge e : graph.getEdges()) {

                    writeWithNewLine(fwrite, "edge [");
                    writeWithNewLine(fwrite, "id " + e.getEdgeId());
                    writeWithNewLine(fwrite, "source " + e.getNode1());
                    writeWithNewLine(fwrite, "target " + e.getNode2()) ;
                    writeWithNewLine(fwrite, "dist " +  e.getDistance());
                    writeWithNewLine(fwrite, "]");

                }




                write(fwrite, "]");
                fwrite.flush();
                fwrite.close();
                System.out.println("Instance " + graph.getId());
                ioThreadLock.release();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
    }


}
