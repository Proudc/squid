#!/bin/bash
javac -encoding utf-8 Edge.java Graph.java Point.java Vertex.java Main.java -d .

nodePath="node.txt"
edgePath="edge.txt"
writePath="/"

java typeOfSigmod.Main ${nodePath} ${edgePath} 0 20000 ${writePath}
