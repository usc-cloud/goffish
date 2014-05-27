import java.io.*;

/**
 * Created by charith on 4/9/14.
 */
public class MetisToLouvanSingle {

    public static void main(String[] args) throws Exception{
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));



        String line = reader.readLine();
        line = reader.readLine();


        int vertexId=1;
        while(line != null) {
            System.out.println(line);
            String []parts = line.split(" ");

            for(String edge:parts) {
                if("".equals(edge.trim()))
                    continue;
                int sink = Integer.parseInt(edge);
                writer.println("" + vertexId + " " + sink);
            }
            vertexId++;
            line = reader.readLine();

        }
        writer.flush();
        writer.close();

    }
}
