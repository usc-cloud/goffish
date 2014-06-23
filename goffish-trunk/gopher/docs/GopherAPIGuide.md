Gopher API Guide
================

Overview
--------

Gopher is a distributed and scalable programming framework which enable running fast
analytics on large time series graphs.Large scale graph analytics is a hot topic both in academia
and industry.People have done significant efforts for development of scalable algorithms,
programming abstractions and execution frameworks. Popularity of Map reduce programming
model and its applications in big­data cause lot of traction which lead to running many graph
analytics on map­reduce based programming frameworks. Recently google pregral and apache
giraph programing model gain more traction in graph analytics community due to ease of
programming and performance benefits compared to other data parallel models like
map­reduce.

Pregal/Apache Graph provides a vertex centric , Bulk Synchronous Parallel(BSP)  programming
abstractions to user where processing is done at vertex level in parallel with several iterations
called super steps. Messages can be passed between vertices in the graph in between these
supersteps.

Gopher compliments this Bulk synchronous parallel programming model  but provides a
subgraph centric programming model to the user. We have observed that this is reduce the
memory pressure and barrier synchronization overhead compared to the vertex centric
abstraction provided by  Google Pregel. Gopher also introduce iterative BSP programming model
where users can compose multiple super­iterations of BSP Jobs with shared state.


Gopher API
------------------
Gopher Provides a Subgraph centric programming abstraction to the user. Implementation of the
subgraph centric application starts with user extending the edu.usc.goffish.gopher.api.GopherSubGraph .

Subgraph centric API:

In subgraph centic programming model users are provided with a subgraph abstraction to
program on. Subgraph is defined as a weekly connected component of a graph. Processing is
done in parallel in each subgraph.Subgraph tasks can communicate with each other using the
subgraph ids. This can be thought as a “Think like a subgraph” programming model with
contrast to a “Think like a vertex”  programming model.

Subgraph centric algorithms executed in interations called super steps seperated by a barrier synchronization step.

*At each super step i each subgraph:
 
 1. Receive messages set to it in super step i-1 

 2. Execute subgraph application logic  

 3. Send messages to other subgraphs (To be received in super step i+1)
 
 4. Can vote to halt : saying i m done/ de-activates

Application comes to a stop once all subgraphs voted to halt. Active subgraphs participate in every superstep. De-active subgraphs participate in computation only if it receives messages which activate that subgraph.

Gopher User API: 
<code>

public abstract class GopherSubGraph {

     /**
     * User implementation logic goes here.
     * To send message to a another partition use {@link #sendMessage(long, SubGraphMessage)}} see
     * {@link SubGraphMessage} for more details about the message format.
     * To signal that current logic is done processing use {@link #voteToHalt()}
     *
     * @param messageList List of SubGraphMessage which is intended for this sub graph.
     */
    public abstract void compute(List<SubGraphMessage> messageList);

    /**
     * User implementation logic goes here.
     *
     * @param messageList  list of message indented for
     */
    public void reduce(List<SubGraphMessage> messageList);


    /**
     * Get the Current super step.
     *
     * @return
     */
    public final int getSuperStep();

      /**
     * Signal that this subgraph finished processing. The system will come to a halt state once all the subgraphs come
     * to an halt state.
     */
    public final void voteToHalt();

     /**
     * Send Message to a given partition.
     *
     * @param partitionId target partition
     * @param message     data message for that partition
     */
    public final void sendMessage(long partitionId, SubGraphMessage message);

    /**
     * Send Message to a subgraph
     *
     * @param message subgraph message
     */
    public final void sendMessage(SubGraphMessage message);
  
     /**
     * Check whether the current sub-graph is in halt state
     *
     * @return
     */
    public final boolean isVoteToHalt();	

} 

</code>

Gopher Messageing: 

In GoFFish graph is initially partitioned into graph partitions. Then connected components in each partition are identified. Gopher Compute nodes work on graph partitions where in each partition user implemented subgraph logic executed in parallel.

Communication between subgraphs are done by exchanging subgraph messages between subgraph tasks. Subgraph message mainly contains a subgraph id which message will be
routed to and data element which contains the data that needs to be transferred. User can choose to send a Subgraph message to a given subgraph by setting the target subgraph id in message or he/she can choose to send a message to all the subgraphs in a given partition by using a overloaded sendMessage method.

Following is the subgraph Message API:

<code>
public class SubGraphMessage implements Serializable {

    /**
     * Create SubGraphMessage providing the data as a byte[]
     * @param data data serialized to byte[] format;
     */
    public SubGraphMessage(byte[] data);
  
     /**
     * Add any application specific tag
     * @param tag application specific tag
     * @return  Current Message Instance
     */
    public SubGraphMessage addTag(String tag);

    /**
     * Set the subgraph id of the target subgraph for this message
     * @param targetSubgraph subgraph id of target subgraph
     */
    public void setTargetSubgraph(long targetSubgraph);
   
     /**
     * Get the message data as a byte[]
     * @return data as a byte[]
     */
    public byte[] getData();

    /**
     * Get the application specific message tag.
     * @return message tag.
     */
    public String getTag();

}
</code>
 
Example Application
------------------

Following is a sample application which finds weakly connected components of a graph. This is
subgraph centric version of connected component identification algorithm listed in apache giraph
This algorithm finds out the smallest vertex id of a connected component at mark that
connection component with that vertex id.
Here the algorithm is very simple. Each subgraph finds the smallest vertex id for each subgraph
and propagate that smallest value to its connected subgraphs. If incoming value to a subgraph is
different from its current value it updates the current value and propagate the changes to its
neighbours.
Following is a sample implementation of the algorithm in gopher.

<code>
 @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {
        try {

            if (getSuperStep() == 0) {


                rids = new ArrayList<>();
                for (ITemplateVertex vertex : subgraph.remoteVertices()) {
                    rids.add(vertex.getId());
                }

                currentMin = subgraph.getId();

                log(partition.getId(), subgraph.getId(), currentMin);

                for (Long rv : rids) {
                    String msg = "" + currentMin;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }

                voteToHalt();
                return;

            }

            boolean changed = false;

            for (SubGraphMessage msg : subGraphMessages) {

                long min = Long.parseLong(new String(msg.getData()));

                if (min < currentMin) {
                    currentMin = min;
                    changed = true;
                }


            }

            // propagate new component id to the neighbors

            if (changed) {

                log(partition.getId(), subgraph.getId(), currentMin);

                for (Long rv : rids) {

                    String msg = "" + currentMin;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }


            }

            voteToHalt();
        } catch (Throwable e) {
            System.out.println(e);
            e.printStackTrace();
        }

    }
</code>


