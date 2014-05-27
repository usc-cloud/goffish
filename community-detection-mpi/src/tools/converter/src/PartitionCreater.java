import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by charith on 4/3/14.
 */
public class PartitionCreater {


    public static void main(String[] args) throws Exception {

        String original = args[0];
        String metisOut = args[1];

        int vertexId=1;
        int numberOfPartitions = Integer.parseInt(args[2]);

        PrintWriter writer[] = new PrintWriter[numberOfPartitions];

        for(int i=0;i<numberOfPartitions;i++) {
            writer[i] = new PrintWriter(new BufferedWriter(new FileWriter(original + "_part_" + (i+1))));
        }

        List<Integer> vertexPartitionMap = new ArrayList<Integer>();


        BufferedReader reader = new BufferedReader(new FileReader(metisOut));

        String line = reader.readLine();

        while(line != null) {

            vertexPartitionMap.add(Integer.parseInt(line));
            line = reader.readLine();

        }

        /**
         * Intermediate format
         * source sink1,partId sink2,partId
         */

        BufferedReader graphReader = new BufferedReader(new FileReader(original));

        line = graphReader.readLine();
        line = graphReader.readLine();

        while (line != null) {
            if(line.contains("#")) {
                line = graphReader.readLine();
                continue;
            }

            int partitionId = vertexPartitionMap.get(vertexId-1);


            writer[partitionId].print(""+vertexId);

            String parts[] = line.split(" ");
            for(String sink: parts) {
                if("".equals(sink.trim())) {
                    continue;
                }
                int s = Integer.parseInt(sink);

                writer[partitionId].print(" " + s + "," + vertexPartitionMap.get(s-1));
            }

            writer[partitionId].print("\n");

            vertexId++;
            line = graphReader.readLine();
        }

    }
}
