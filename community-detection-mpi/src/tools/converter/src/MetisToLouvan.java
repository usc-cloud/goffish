import java.io.*;
import java.util.*;

/**
 * Created by charith on 4/4/14.
 */
public class MetisToLouvan {


    /**
     * This will take 3 input arguments
     * @param args args[0]= original graph, args[1]=partition file, arg[2]= number of partitions
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        BufferedReader graphReader = new BufferedReader(new FileReader(args[0]));
        BufferedReader partitionReader = new BufferedReader(new FileReader(args[1]));

        int numberOfPartitions = Integer.parseInt(args[2]);
        PrintWriter []writer = new PrintWriter[numberOfPartitions];
        PrintWriter []remoteWriter = new PrintWriter[numberOfPartitions];

        for(int i=0; i < numberOfPartitions;i++ ) {

            writer[i] = new PrintWriter(new BufferedWriter(new FileWriter(args[0]+ "_" + i + ".out")));
            remoteWriter[i] = new PrintWriter(new BufferedWriter(new FileWriter(args[0]+"_" + i + ".remote")));
        }


        List<Integer> vertexPartitionMap = new ArrayList<Integer>();
        String line = partitionReader.readLine();

        while(line != null) {

            vertexPartitionMap.add(Integer.parseInt(line));
            line = partitionReader.readLine();

        }



        line = graphReader.readLine();
        line = graphReader.readLine();
        int vertexId =1;
        while (line != null) {
            if(line.contains("#")) {
                line = graphReader.readLine();
                continue;
            }

            int partitionId = vertexPartitionMap.get(vertexId-1);




            String parts[] = line.split(" ");
            for(String sink: parts) {
                if("".equals(sink.trim())) {
                    continue;
                }
                int s = Integer.parseInt(sink);

                if( partitionId != vertexPartitionMap.get(s-1)) {
                    //remote vertex
                    remoteWriter[partitionId].println("" + vertexId + " " + s);

                } else {
                    //local
                    writer[partitionId].println("" + vertexId + " " + s);


                }
            }

            vertexId++;
            line = graphReader.readLine();
        }

        for(int i=0; i < numberOfPartitions;i++ ) {

            writer[i].flush();
            writer[i].close();
            remoteWriter[i].flush();
            remoteWriter[i].close();
        }



    }
}
