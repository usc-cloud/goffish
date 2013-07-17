package com.pfl.dynamicgraphplayer;

import com.pfl.dynamicgraphplayer.DynamicGraphPlayerTopComponent.AnimatorStateListener;
import edu.usc.pgroup.goffish.gofs.ISubgraphInstance;
import edu.usc.pgroup.goffish.gofs.ISubgraphTemplate;
import edu.usc.pgroup.goffish.gofs.subgraph.TemplateEdge;
import edu.usc.pgroup.goffish.gofs.subgraph.TemplateVertex;
import java.awt.Point;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import org.gephi.graph.api.Edge;
import org.gephi.graph.api.Graph;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.project.api.ProjectController;
import org.gephi.utils.longtask.spi.LongTask;
import org.gephi.utils.progress.ProgressTicket;
import org.openide.util.Lookup;


//This class thread allows for the ability to load massive graphs in RAM and query on individual subgraphs for display
public class AnimateBuildout implements LongTask,Runnable{

    TreeMap<Long, ISubgraphInstance> prevActiveInstance = null;
    private List<Long> sortedKeys;
    private void updateNodes(Long interimDate, GraphModel graphModel,Graph grph, HashMap<Long, ArrayList<Node>> hashTimeout, double basecount) {
        
        TreeMap<Long, ISubgraphInstance> activeInstance = getActiveInstance(interimDate);
        System.out.println(interimDate + " " + activeInstance.size());
        
        if(prevActiveInstance != activeInstance)
        {
            grph.clear();
            if(grph.getNodeCount() == 0)
            {
                //Load Template
                for(Long sgid : activeInstance.keySet())
                {                
                    ISubgraphInstance subgraphInstance = activeInstance.get(sgid);
                    float r = 1/(float)sgid;//(float) Math.random();
                    float g = 1/(float)sgid*2;//(float) Math.random();
                    float b = 0;//(float) Math.random();
                    for(TemplateVertex vertex: subgraphInstance.getTemplate().vertices())
                    {
                        //createNode(String nodeValue, Long timeStamp, GraphModel graphModel, Graph grph, HashMap<Long, ArrayList<Node>> hashTimeout, long basecount)
                        Node n = createNode(String.valueOf(vertex.getId()),interimDate,graphModel,grph,hashTimeout,basecount,r,g,b);
                        String label = (String) subgraphInstance.getPropertiesForVertex(vertex.getId()).getValue("license-list");
                        System.out.println("lbl"+label);
                        n.getNodeData().setLabel(label);
                    }

                    for(TemplateEdge edge: subgraphInstance.getTemplate().edges())
                    {
                        Node src = grph.getNode(String.valueOf(edge.getSource().getId()));
                        Node sink = grph.getNode(String.valueOf(edge.getSink().getId()));
                        Edge e = graphModel.factory().newEdge(String.valueOf(edge.getId()), src,sink, (int)subgraphInstance.getPropertiesForEdge(edge.getId()).getValue("weight"), true);

                        grph.addEdge(e);
                    }
                }            
            }
            else
            {
                //Update with instance values
                activeInstance = getActiveInstance(interimDate);
                if(prevActiveInstance != activeInstance)
                {
                    for(ISubgraphInstance subgraphInstance : activeInstance.values())
                    {                
                        for(TemplateVertex vertex: subgraphInstance.getTemplate().vertices())
                        {
                            //createNode(String nodeValue, Long timeStamp, GraphModel graphModel, Graph grph, HashMap<Long, ArrayList<Node>> hashTimeout, long basecount)
                            Node node = grph.getNode(String.valueOf(vertex.getId()));
                            String label = (String) subgraphInstance.getPropertiesForVertex(vertex.getId()).getValue("license-list");
                            System.out.println("lbl"+label);
                            node.getNodeData().setLabel(label);
                            //node.getNodeData().setColor((float)Math.random(),(float)Math.random(),(float)Math.random());
                            //Change Node properties here.. 
                            //TODO
                        }

                        for(TemplateEdge edge: subgraphInstance.getTemplate().edges())
                        {                    
                            Edge e = grph.getEdge(String.valueOf(edge.getId()));
                            int weight = (int) subgraphInstance.getPropertiesForEdge(edge.getId()).getValue("weight")*10;

                            e.setWeight(weight);
                            //e.getEdgeData().setColor((float)Math.random(),(float)Math.random(),(float)Math.random());
                        }
                    }
                }


            }  
        }
        prevActiveInstance = activeInstance;
    }

    private TreeMap<Long, ISubgraphInstance> getActiveInstance(Long interimDate) {
        TreeMap<Long, ISubgraphInstance> instance = null;
        TreeMap<Long, ISubgraphInstance> previous = null;
        TreeMap<Long, ISubgraphInstance> current = null;
        
        Long timeStampReturned = 0L;
        Long previousTS = 0L;
        for(Long timeStamp : sortedKeys)
        {
            current = _graphData._timeseriesGraph.get(timeStamp);
            
            if(timeStamp>=interimDate)
            {
                timeStampReturned = previousTS;
                instance = previous;
                break;
            }
            previous = current;
            previousTS = timeStamp;
        }
        System.out.println("TS: " + timeStampReturned);
        return instance;
    }

    
   public enum events{
       UPDATE_DATE,
       DATE_PROGRESS,
       UPDATE_MAX_DENSITY,
       PROMOTED_NODE,
       REENTRANT_NODE,
       TOTAL_VALUE,
       DATE_SPAN,
       UPDATE_TRIANGLES,
       UPDATE_DENSITY
   }

    private PropertyChangeSupport pcs = new PropertyChangeSupport(this);
    private int propertyFullSpan = -1;

    public long TIMEOUT_LENGTH = (long)1000*60*60*24*7;
    
    // length of each tick in seconds
    public int TICK_LENGTH = 1; // one sec
   
    //Graph hash table repository that we query against
    private GMLLoader _graphData;
  
    // determine if the size of the node is realted to the number of records or the total ammount
    public String SizeSelect="Records";
    //public String SizeSelect="Amount";
    
    
    //whether we are trying to actively kill the thread
    private boolean _killthread = false;
    
    //whether the thread is halted 
    private boolean _killed = true;
        
    //List of all template nodes
    List<Node> nodeList;
    
    //Super important: Hash table to store the age of each node's components (a node may have multiple entries)
    //Nodes are added with a date timestamp.  Their size will inform us whether the node needs its diameter resized
    //or whether the node needs to be removed entirely.
    //Long is the timestamp
    HashMap<Long, ArrayList<Node>> hashTimeout;
    
    private AnimatorStateListener stateListener;
 
    private float _maxDensity;
    
    
    
    /**
     * CONSTRUCTOR
     * @param graphdata  hash tables loaded from data file
     * @param milliDelay 
     */
    AnimateBuildout(GMLLoader graphData, int milliDelay)
    { 
        _graphData = graphData;
        _millisecondDelay = milliDelay;
        _maxDensity = 0;    
    }
    
    
    /////////////////////////////////////////////
    //<Getters and Setters>
    /////////////////////////////////////////////
    public void addAnimationChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }
    
   
    public void raiseCurrentDate(String propertyCurrentDate)
    {
        pcs.firePropertyChange(events.UPDATE_DATE.toString(), null, propertyCurrentDate);    
    }
    
    
    //Get and set routines for the sleep time used in animation
    private int _millisecondDelay = 50;
    public void setDelay(int newDelay)
    {
        _millisecondDelay = newDelay;
    }
    
    //Get and set routines for the Date function
    private Long _minDate;
    public Long getDate()
    {
       return _minDate;
    }
    
    
    //This function lets someone start the animation from an arbitrary point in time
    //Variable to store where we currently are (IE: if paused)
    private Long _pauseDate;
    public void setPauseDate(Long pauseDate)
    { 
        //Setting this externally - clear it all out!
        _pauseDate = pauseDate;
        clearGraph();
        
    }
    
    
    public void clearGraph(){
        //Next clear off the graph helper lists.
        //First, clear out the list of secondary nodes that we are going to query and add
        nodeList.clear();
        
        //Second, clear out the list with all the timeout data that records when nodes should be removed
        hashTimeout.clear();

        //Third clear out the reentrant list
        //hashReentrant.clear();
        
        //Get the graph object
        Graph grph = getGraph();
        
        if(grph!=null)
        {
            //Finally - all of this is to clear off the graph
            grph.clear();
        }
    }
    
    
    private Graph getGraph()
    {
        //Get our project / workspace / and graph model.  
        ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
        if(pc.getCurrentProject() == null)
        {
            pc.newProject();
        }
        
        //Start by initializing the gra
        GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getModel();
        if(graphModel == null)
        {
            return null;
        }
        
        Graph grph;
        grph = graphModel.getGraph();
        return grph;
    }
    
    public boolean isRunning()
    {
        if (_killed)
        {
            return false;
        }
        else
        {
            return true;
        }
    }
    
   
    
    @Override
    public boolean cancel() {
        _killthread = true;
        return true;
    }

    @Override
    public void setProgressTicket(ProgressTicket pt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
   
  
    
    private Node createNode(String nodeValue, Long timeStamp, GraphModel graphModel, Graph grph, HashMap<Long, ArrayList<Node>> hashTimeout, double basecount, float r, float g, float b)
    {
        Node newNode = grph.getNode(nodeValue);
        
        float coreColor = 1;
        
        if (newNode == null)
        {
            newNode = graphModel.factory().newNode(nodeValue);
            
            
            /*if(hashReentrant.contains(newNode.toString()))
            {
                final String nodeText1 = newNode.toString();
                raiseReentrantNode(nodeText1, basecount);    
            }*/
            
            grph.addNode(newNode);
            
            newNode.getNodeData().setSize(1);
            newNode.getNodeData().getAttributes().setValue("Presence", 1);
            
            newNode.getNodeData().setColor((float)r, (float)g, (float)b);            
        }
        else
        {
            newNode.getNodeData().getAttributes().setValue("Presence", (Integer)newNode.getNodeData().getAttributes().getValue("Presence") + 1);
        }
        
        if(!hashTimeout.containsKey(timeStamp))
        {
             hashTimeout.put(timeStamp, new ArrayList<Node>());
        }
        
        //Add the node to the array for this time window
        hashTimeout.get(timeStamp).add(newNode);
        nodeList.add(newNode);
        return newNode;
    }
    
    @Override
    public void run() {
        _killthread = false;
        _killed = false;
        
        // TODO this is probably not the best place to do the workspace management piece
        ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
        if(pc.getCurrentProject() == null)
        {
            pc.newProject();
        }
       
        
        //Start by initializing the graph
        GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getModel();
        if(graphModel == null)
        {
            return;
        }
        Graph grph;
        grph = graphModel.getGraph();
        
        if(_pauseDate == null)
        {
            grph.clear();
        }
        
        TreeMap<Long,TreeMap<Long, ISubgraphInstance>> senderHash = _graphData._timeseriesGraph;
        
        sortedKeys = new ArrayList(senderHash.keySet());
        Collections.sort(sortedKeys);
        
        
        Long prevDate = null;
        Long maxDate = sortedKeys.get(sortedKeys.size()-1)+1000*10;
        Long minDate = sortedKeys.get(0)-1000*10;
        _minDate = minDate;
        long fullSpan =  (maxDate - minDate)/(1000*TICK_LENGTH);
        sortedKeys.add(maxDate);       
        Collections.sort(sortedKeys);
        
        // use a -1 so the event should always fire
        pcs.firePropertyChange(events.DATE_SPAN.toString(),this.propertyFullSpan,(int)fullSpan);
        this.propertyFullSpan =  (int) fullSpan;
        
        
        double basecount = 0;
        if (_pauseDate == null || grph.getNodeCount()== 0)
        {
            nodeList = new ArrayList<Node>();
            hashTimeout = new HashMap<Long, ArrayList<Node>>();
        }
        if(_pauseDate!=null)
        {
            final int sliderPosition = (int)(_pauseDate-minDate)/(1000*TICK_LENGTH);
            pcs.firePropertyChange(events.DATE_PROGRESS.toString(), null, sliderPosition);
            
            
            //Set the slider bar to start in the correct place
            
            basecount = (_pauseDate-minDate)/ 1000.0 / TICK_LENGTH;
        }
      
        for(Long dt : sortedKeys)
        {   
            
            //calculateTriangles((int)basecount, grph);
            //calculateTotalValue((int)basecount);
            //calculateDensity((int)basecount, grph);
            
            if(_pauseDate != null)
            {
                if(dt - _pauseDate <= 0)
                {
                    continue;
                }
            }
            long timedelta = 0;
            double timedeltaRemainder = 0;
            
            if (prevDate !=null)
            {
                timedelta = dt - prevDate;
                //Calculate seconds / minutes/ hours/ days
                timedeltaRemainder = timedelta / 1000.0 % TICK_LENGTH / TICK_LENGTH;
                timedelta = timedelta /1000/TICK_LENGTH;                
            }
            try
            {
                if(timedelta > 0 || timedeltaRemainder > 0)
                {
                    if (timedelta > 0) basecount++;
                    basecount += timedeltaRemainder;
                    pcs.firePropertyChange(events.DATE_PROGRESS.toString(), null, (int) basecount);
                    
                    Long interimDate = prevDate;
                    Thread.sleep(_millisecondDelay);    
                    if(timedelta > 1)
                    {
                        //Need a -1 because we skip the prevdate and we also skip the actual dt.  Don't want to duplicate!
                        for(int cntr = 0; cntr < (timedelta); cntr ++)
                        {
                            
                            basecount++;
                            pcs.firePropertyChange(events.DATE_PROGRESS.toString(), null, (int) basecount);
                            
                            interimDate = interimDate + 1000*TICK_LENGTH;
                
                            //grph.writeLock();
                            //Since, we have that a fixed structure, we do not need removeOldNodes
                            //removeOldNodes(hashTimeout, interimDate, grph);
                            
                            
                            //Update Nodes
                            updateNodes(interimDate,graphModel,grph,hashTimeout,basecount);
                            
                            //Load all secondary nodes:
                                    
                            raiseCurrentDate(interimDate.toString());
                            
                            //calculateTriangles((int)basecount, grph);
                            //calculateTotalValue((int)basecount);
                            //calculateDensity((int)basecount, grph);
                            
                            if(!_killthread){
                                // if there has been a request to kill the thread don't sleep
                                 // just finish the method and exit as fast as we can.
                                 Thread.sleep(_millisecondDelay); 
                             }
                            
                        }
                    }
                    
                    DateFormat dateFormatter = DateFormat.getDateInstance(DateFormat.DEFAULT, Locale.ENGLISH); 
                    raiseCurrentDate(dateFormatter.format(interimDate));
                }
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
                e.printStackTrace();
                if (this.stateListener != null){
                    stateListener.onError(e.getMessage());
                }
            }
            //grph.writeLock();
            prevDate = dt;

            if(_killthread)
            {
                _killed = true;
                _pauseDate = dt;
                return;
            }
        }

    }
    
    
    
    public void setStateListener(AnimatorStateListener listener){
        this.stateListener = listener;
    }

   
    
     //This function will calculate the number of 1st degree triangles and feed this info back out for visualization
    private void calculateTriangles(int xCoord, Graph grph)
    {
        
        HashSet<Node> ndSet = new HashSet<Node>();
        int num_triangles = 0;
        
        for(Node nd : nodeList)
        {
            if ((Integer)nd.getAttributes().getValue("Depth")==1)
            {
                ndSet.add(nd);
            }
        }
        
        for(Node nd : ndSet)
        {
            for (Node nd2: ndSet)
            {
                if (nd != nd2)
                {
                    //For every edge between these - we have one triangle
                    if(grph.getEdge(nd, nd2) != null)
                    {
                        num_triangles++;
                    }
                }
            }
        }
        
        final Point graphPoint = new Point(xCoord,num_triangles);
        pcs.firePropertyChange(events.UPDATE_TRIANGLES.toString(),null,graphPoint);     
        
    }
    
    
    
     private void calculateTotalValue(int xCoord)
    {
        int totalValue=0;
        for(Long lng : hashTimeout.keySet())
        {
            for(Node nd : hashTimeout.get(lng))
            {
                if(nd.getAttributes().getValue("NetAmount")!=null)
                {
                    totalValue += (Integer)nd.getAttributes().getValue("NetAmount");
                }
            }
        }
        
        
        final Point p = new Point(xCoord,totalValue);
        pcs.firePropertyChange(events.TOTAL_VALUE.toString(), null, p);
       
    }
     
     
    private void calculateDensity(int xcoord, Graph grph)
    {
        
        if(grph.getNodeCount() < 2 || xcoord < 200)
        {
            return;
        }
        final int num_t = 10000*(2*grph.getEdgeCount())/(grph.getNodeCount()*(grph.getNodeCount()-1));
        if (num_t > _maxDensity)
        {
            _maxDensity = num_t;
            final String maxDensity = String.valueOf(num_t/(float)10000.0);
            pcs.firePropertyChange(events.UPDATE_MAX_DENSITY.toString(), null, maxDensity);
            
        }
        
        final Point graphPoint = new Point(xcoord,num_t);
        pcs.firePropertyChange(events.UPDATE_DENSITY.toString(), null, graphPoint);
      
    }
    
    
    public void raisePromotedNode(int propertyPromotedNodeXC)
    {
        pcs.firePropertyChange(events.PROMOTED_NODE.toString(), null, propertyPromotedNodeXC);
    }
    
    
    public void raiseReentrantNode(String propertyReentrantNode, long xc)
    {       
        final Point p = new Point( (int) xc,1);
        pcs.firePropertyChange(events.REENTRANT_NODE.toString(), null, p);

    }
    
}
