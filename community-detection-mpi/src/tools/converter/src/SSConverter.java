import com.sun.javaws.exceptions.InvalidArgumentException;

import java.io.*;
import java.util.*;

/**
 * Created by charith on 4/3/14.
 */
public class SSConverter {


    static SortedMap<Integer, List<Integer>> treeMap = new TreeMap<Integer, List<Integer>>();
    static List<Integer> verts = new ArrayList<Integer>();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new InvalidArgumentException(args);
        }

        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));


        String line = reader.readLine();

        while (line != null) {
            // System.out.println(line);
            if (line.contains("#")) {
                line = reader.readLine();
                continue;
            }


            String[] tokens = line.split("\t");


            // if(st.hasMoreTokens()) {
            int source = Integer.parseInt(tokens[0]);
            int sink = Integer.parseInt(tokens[1]);
            //   System.out.println(" " + source + " " + sink);
            if (treeMap.containsKey(source)) {
                treeMap.get(source).add(sink);
            } else {
                List<Integer> list = new ArrayList<Integer>();
                list.add(sink);
                treeMap.put(source, list);
            }

            //}


            line = reader.readLine();


        }

        System.out.println(" Reading done...");
        Iterator<Integer> it = treeMap.keySet().iterator();

        Map<Integer, List<Integer>> newEdges = new TreeMap<Integer, List<Integer>>();
        while (it.hasNext()) {

            int source = it.next();

            List<Integer> sinks = treeMap.get(source);

            for (int e : sinks) {

                if (!containsEdge(e, source)) {
                    if (newEdges.containsKey(e)) {
                        newEdges.get(e).add(source);
                    } else {
                        List<Integer> list = new ArrayList<Integer>();
                        list.add(source);
                        newEdges.put(e, list);
                    }
                }
            }

        }


        //update the tree map

        Iterator<Integer> newIt = newEdges.keySet().iterator();

        while (newIt.hasNext()) {
            int s = newIt.next();
            List<Integer> edges = newEdges.get(s);

            for (int e : edges) {

                if (treeMap.containsKey(s)) {
                    treeMap.get(s).add(e);
                } else {
                    List<Integer> integerList = new ArrayList<Integer>();
                    integerList.add(e);
                    treeMap.put(s, integerList);
                }

            }
        }

        System.out.println(" Undirecting done...");

        it = treeMap.keySet().iterator();
        int vs = 0;
        int es = 0;
        while (it.hasNext()) {

            int source = it.next();

            if (!verts.contains(source)) {
                vs++;
            }

            List<Integer> edges = treeMap.get(source);
            Collections.sort(edges);

            for (int e : edges) {
                if (!verts.contains(e)) {
                    vs++;
                }
                es++;
            }
        }

        writer.println("" + vs + " " + es/2);
        System.out.println("" + vs + " " + es);
        it = treeMap.keySet().iterator();
        while (it.hasNext()) {

            int source = it.next();
            List<Integer> edges = treeMap.get(source);
            Collections.sort(edges);
            boolean first = true;
            for (int e : edges) {
                if (!first)
                    writer.print(" " + e);
                else {
                    first = false;
                    writer.print(e);
                }
            }
            writer.print("\n");

        }
        System.out.println("Done..");
        writer.flush();
        writer.close();


    }


    public static boolean containsEdge(int source, int sink) {
        boolean result = false;
        if (treeMap.containsKey(source)) {
            return treeMap.get(source).contains(sink);
        }


        return result;
    }

}
