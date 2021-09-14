package src.main.scala.util.trajic;

import java.util.ArrayList;
import java.util.Queue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class Huffman {

    public static Node createTree(double[] freqs, int nFreqs) {
        PriorityQueue<Node> priorityQueue = new PriorityQueue<Node>(10, new Comparator<Node>() {
            @Override
            public int compare(Node n1, Node n2) {
                double value1 = n1.getFrequency();
                double value2 = n2.getFrequency();
                if (value1 > value2) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
        
        for (int i = 0; i < nFreqs; i++) {
            priorityQueue.add(new Node(i, freqs[i]));
        }

        while(priorityQueue.size() > 1) {
            Node left  = priorityQueue.poll();
            Node right = priorityQueue.poll();
            priorityQueue.add(new Node(left, right));
        }

        return priorityQueue.poll();
    }

    public static ArrayList<String> canonicalize(TemPair[] cws, int nCodewords) {
        Arrays.sort(cws, 0, nCodewords, new Comparator<TemPair>(){
            @Override
            public int compare(TemPair p1, TemPair p2) {
                return p1.second.length() - p2.second.length();
            }
        });

        int code = 0;
        for (int i = 0; i < nCodewords; i++) {
            String strCode = Integer.toBinaryString(code);
            ArrayList<Boolean> bs = new ArrayList<>(cws[i].second.length());
            for (int j = 0; j < cws[i].second.length(); j++) {
                bs.add(Boolean.FALSE);
            }
            for (int j = strCode.length() - 1; j >= 0; j--) {
                if (strCode.charAt(j) == '1') {
                    int index = bs.size() - 1 - (strCode.length() - 1 - j);
                    bs.set(index, Boolean.TRUE);
                }
            }
            String binary = "";
            for (int j = 0; j < cws[i].second.length(); j++) {
                binary = bs.get(j) == Boolean.FALSE ? binary + "0" : binary + "1";
            }
            cws[i] = new TemPair(cws[i].first, binary);
            if (i < nCodewords - 1) {
                code = (code + 1) << (cws[i + 1].second.length() - cws[i].second.length());
            }
        }

        Arrays.sort(cws, 0, nCodewords, new Comparator<TemPair>() {
            @Override
            public int compare(TemPair p1, TemPair p2) {
                return p1.first - p2.first;
            }
        });

        ArrayList<String> codewords = new ArrayList<String>(nCodewords);
        for (int i = 0; i < nCodewords; i++) {
            codewords.add(null);
        }
        for (int i = 0; i < nCodewords; i++) {
            codewords.set(i, cws[i].second);
        }
        return codewords;

    }
    
    public static ArrayList<String> createCodewords(double[] freqs, int nFreqs) {
        ArrayList<String> codewords = new ArrayList<String>(nFreqs);
        for (int i = 0; i < nFreqs; i++) {
            codewords.add(null);
        }
        Queue<TemNodeCode> q = new LinkedList<>();
        Node root = createTree(freqs, nFreqs);
        q.add(new TemNodeCode(root, ""));
        while (!q.isEmpty()) {
            TemNodeCode nc = q.poll();
            if(nc.node.isLeaf()) {
                codewords.set(nc.node.data, nc.str);
            } else {
                q.add(new TemNodeCode(nc.node.left, nc.str + "0"));
                q.add(new TemNodeCode(nc.node.right, nc.str + "1"));
            }
        }
        TemPair[] cws = new TemPair[nFreqs];
        for (int i = 0; i < nFreqs; i++) {
            cws[i] = new TemPair(i, codewords.get(i));
        }

        return canonicalize(cws, nFreqs);
    }

}

class TemNodeCode {
    
    public Node node;

    public String str;

    public TemNodeCode(Node node, String str) {
        this.node = node;
        this.str = str;
    }
}

class TemPair {

    public int first;

    public String second;

    public TemPair(int first, String second) {
        this.first = first;
        this.second = second;
    }

}
