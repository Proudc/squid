package sampleTraj;

import typeOfSigmod.Edge;
import typeOfSigmod.Vertex;
import typeOfSigmod.Point;
import typeOfSigmod.Graph;

import java.util.ArrayList;

public class Main{
    public static void main(String[] args){
        String nodePath = args[0];
        String edgePath = args[1];
        int beginTrajCount = Integer.valueOf(args[2]);
        int endTrajCount = Integer.valueOf(args[3]);
        String writePath = args[4];

        Graph graph = new Graph();
        graph.readVertexs(nodePath);
        graph.readEdges(edgePath);
        String rootReadPath = "";
        String rootWritePath = "";
        graph.sampleTrajAccordRandomWalk(beginTrajCount, endTrajCount, rootReadPath, rootWritePath);
    }
}