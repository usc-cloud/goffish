import java.io.*;
import java.util.*;

/**
 * Created by charith on 3/20/14.
 */
public class Converter {

    static SortedMap<Integer,List<Integer>> sortedMap = new TreeMap<Integer, List<Integer>>();

    static Map<Integer,Integer> vertexMap = new HashMap<Integer, Integer>();


    public static void main(String[] args) throws Exception{

        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));


        int vertexCount=0;
        int edgeCount=0;

        String line = reader.readLine();
        while (line != null) {

            String parts[] = line.split(" ");
            int source = Integer.parseInt(parts[0]);
            int sink = Integer.parseInt(parts[1]);

            if(sortedMap.containsKey(source)) {
                sortedMap.get(source).add(sink);
            } else {
                List<Integer> list = new ArrayList<Integer>();
                list.add(sink);
                sortedMap.put(source,list);
            }

            if(sortedMap.size() > 1000) {
                break;
            }
            line = reader.readLine();
        }


        Iterator<Integer> sourceIt = sortedMap.keySet().iterator();

        Map<Integer,Integer> vtoOrig = new HashMap<Integer, Integer>();

        int sourceId=0;

        while(sourceIt.hasNext()) {
            int orig = sourceIt.next();
            vtoOrig.put(orig,sourceId++);
        }


        Iterator<Integer> sourceItNew = sortedMap.keySet().iterator();

        while(sourceItNew.hasNext()) {
            int source = sourceItNew.next();
            int sourceMap = vtoOrig.get(source);

            List<Integer> edges = sortedMap.get(source);

            for(int e: edges) {
                int newE=-1;
                if(vtoOrig.containsKey(e)) {
                    newE = vtoOrig.get(e);
                } else {
                    newE = sourceId++;
                    vtoOrig.put(e,newE);
                }

                writer.println("" + sourceMap + " " + newE + " 1");
            }
        }




        writer.flush();
        writer.close();


    }
}
