package edu.usc.pgroup.gml.generator;

public class VehicleMovement {
	private long source;

	private long sink;

	private long edgeId;
	
	public VehicleMovement(long source, long sink, long edgeId){
		this.source = source;
		this.sink = sink;
		this.edgeId = edgeId;
	}

	public void setSource(long source) {
		this.source = source;
	}

	public void setSink(long sink) {
		this.sink = sink;
	}

	public void setEdgeId(long edgeId) {
		this.edgeId = edgeId;
	}

	public long getSource() {
		return source;
	}

	public long getSink() {
		return sink;
	}

	public long getEdgeId() {
		return edgeId;
	}
}
