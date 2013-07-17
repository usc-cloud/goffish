package com.pfl.dynamicgraphplayer;


import com.pfl.dynamicgraphplayer.DynamicGraphPlayerTopComponent.FileLoaderStateListener;
import edu.usc.pgroup.goffish.gofs.IInstanceSerializablePartition;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TreeMap;
import org.gephi.utils.longtask.spi.LongTask;
import org.gephi.utils.progress.Progress;
import org.gephi.utils.progress.ProgressTicket;
import org.gephi.utils.progress.ProgressTicketProvider;
import org.openide.util.Lookup;
import edu.usc.pgroup.goffish.gofs.ISliceManager;
import edu.usc.pgroup.goffish.gofs.ISubgraph;
import edu.usc.pgroup.goffish.gofs.ISubgraphInstance;
import edu.usc.pgroup.goffish.gofs.ISubgraphTemplate;
import edu.usc.pgroup.goffish.gofs.partition.gml.GMLFileIterable;
import edu.usc.pgroup.goffish.gofs.partition.gml.GMLPartition;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kumbhare
 */
public class GMLLoader implements LongTask, Runnable {

    //Build out a hash of Date -> SubGraphId -> SubgraphInstance
    public TreeMap<Long, TreeMap<Long, ISubgraphInstance>> _timeseriesGraph;
    public List<ISubgraphTemplate> _subGraphTemplateList;
    
    
    private FileLoaderStateListener stateListener;
    SimpleDateFormat _dateFormat;
    boolean ignoreHeaders;
    

    private ISliceManager sliceManager;
    public IInstanceSerializablePartition gmlPartition;

    private Path tempPath;
    private final InputStream _templateStream;
    private final ArrayList<Path> _paths;
    
    GMLLoader(InputStream templateStream, ArrayList<Path> paths, SimpleDateFormat dateFormat) {
        super();
        _templateStream = templateStream;
        _paths = paths;
        _dateFormat = dateFormat;
    }
    

    @Override
    public boolean cancel() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setProgressTicket(ProgressTicket pt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void run() {

        _timeseriesGraph = new TreeMap<>();
        _subGraphTemplateList = new ArrayList<>();
        
        ProgressTicketProvider progressProvider = Lookup.getDefault().lookup(ProgressTicketProvider.class);
        ProgressTicket progressTicket = null;
        if (progressProvider != null) {
            progressTicket = progressProvider.createTicket("Loading Graph Data...", null);
        }
        Progress.start(progressTicket);

        gmlPartition = GMLPartition.parseGML(1, _templateStream, new GMLFileIterable(_paths));
        Progress.progress(progressTicket);
        //tempPath = Files.createTempDirectory("slices");
        //Progress.progress(progressTicket);
        //sliceManager = new SliceManager(UUID.randomUUID(), new JavaSliceSerializer(), new FileStorageManager(tempPath), null);
        //Progress.progress(progressTicket);
        
        for (Iterator<ISubgraph> it = gmlPartition.iterator(); it.hasNext();) {
            ISubgraph subGraph = it.next();
            _subGraphTemplateList.add(subGraph.getTemplate());
        }


        Iterable<List<Map<Long, ? extends ISubgraphInstance>>> instances = gmlPartition.getSubgraphsInstances();

        for(List<Map<Long, ? extends ISubgraphInstance>> instanceListForSlice: instances)
        {
            for(Map<Long, ? extends ISubgraphInstance> subGraphsInstances: instanceListForSlice)
            {
                for(Long subGraphId:subGraphsInstances.keySet())
                {
                    ISubgraphInstance subGraphInstance = subGraphsInstances.get(subGraphId);
                    if(!_timeseriesGraph.containsKey(subGraphInstance.getTimestampStart()))
                    {
                        _timeseriesGraph.put(subGraphInstance.getTimestampStart(), new TreeMap<Long, ISubgraphInstance>());
                    }
                    _timeseriesGraph.get(subGraphInstance.getTimestampStart()).put(subGraphId, subGraphInstance);
                }
            }
        }
        
        Progress.finish(progressTicket);

        if (stateListener != null){
             stateListener.onSuccess();
        }
    }
    
    
    public void setStateListener(FileLoaderStateListener listener){
        this.stateListener = listener;
    }

    void setIgnoreHeaders(boolean selected) {
        ignoreHeaders = selected;
    }
}
