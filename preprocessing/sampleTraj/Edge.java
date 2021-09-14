package sampleTraj;

public class Edge {
    public int     edgeID;
    public int     firstVertexID;
    public int     secVertexID;
    public Vertex  firstVertex;
    public Vertex  secVertex;
    public double  length;
    public double  distance;  
    public double  realDistance;
    public double  travelTime;
    public boolean referenceFlag=false;

    public Edge(){}

    public Edge(int edgeID, Vertex firstVertex, Vertex secVertex){
        this.edgeID        = edgeID;
        this.firstVertex   = firstVertex;
        this.secVertex     = secVertex;
        this.firstVertexID = firstVertex.vertexID;
        this.secVertexID   = secVertex.vertexID;
        setEdgeLength();
        setEdgeRealDistance();
        // 16 means 16m/s
        this.travelTime    = this.realDistance / 16;
    }

    public void setTravelTime(float travelTime){
        this.travelTime = travelTime;
    }

    public void setEdgeLength(){
        double px = firstVertex.longitude;
        double py = firstVertex.latitude;
        double qx = secVertex.longitude;
        double qy = secVertex.latitude;
        double x  = px - qx;
        double y  = py - qy;
        this.length = Math.sqrt(x * x + y * y);
    }

    public void setEdgeRealDistance(){
        double px             = firstVertex.longitude;
        double py             = firstVertex.latitude;
        double qx             = secVertex.longitude;
        double qy             = secVertex.latitude;
        double lonDiff        = Math.abs(px - qx);
        double latDiff        = Math.abs(py - qy);
        double lonChangeRatio = Math.cos(py);
        double lonDistance    = lonDiff * 111111 * lonChangeRatio;
        double latDistance    = latDiff * 111111;
        this.realDistance     = Math.sqrt(lonDistance * lonDistance + latDistance * latDistance);
    }

    public static double realDistance(double lonDiff, double latDiff, double lat){
        double lonChangeRatio = Math.cos(lat);
        double lonDistance    = lonDiff * 111111 * lonChangeRatio;
        double latDistance    = latDiff * 111111;
        return Math.sqrt(lonDistance * lonDistance + latDistance * latDistance);
    }
}