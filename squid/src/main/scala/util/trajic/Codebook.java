package src.main.scala.util.trajic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class Codebook {
    
    public ArrayList<Integer> alphabet;

    public ArrayList<String> codewords;

    public Node root;

    public Codebook(ArrayList<Integer> alphabet, ArrayList<String> codewords) {
        this.alphabet  = alphabet;
        this.codewords = codewords;
        buildTree();
    }

    public Codebook(ArrayList<Integer> alphabet, Ibstream ibs) {
        this.alphabet = alphabet;
        int nCodewords = alphabet.size();
        int maxLen = ibs.readByte();
        TemPair[] cws = new TemPair[nCodewords];
        for (int i = 0; i < nCodewords; i++) {
            long len = ibs.readInt(maxLen);
            String binary = "";
            for (int j = 0; j < len; j++) {
                binary += "0";
            } 
            cws[i] = new TemPair(i, binary);
        }
        this.codewords = Huffman.canonicalize(cws, nCodewords);
        buildTree();
    }

    private void buildTree() {
        this.root = new Node();
        for (int i = 0; i < this.alphabet.size(); i++) {
            Node node = this.root;
            String temStr = this.codewords.get(i);
            for (int j = 0; j < temStr.length(); j++) {
                char c = temStr.charAt(j);
                Node n = c == '0' ? node.left : node.right;
                if (n == null) {
                    n = new Node();
                    if (c == '0') {
                        node.left = n;
                    } else {
                        node.right = n;
                    }
                }
                node = n;
            }
            node.data = this.alphabet.get(i);
        }
    }

    public ArrayList<Integer> getAlphabet() {
        return this.alphabet;
    }

    public ArrayList<String> getCodewords() {
        return this.codewords;
    }

    public int lookup(Ibstream ibs) {
        Node node = this.root;
        while(!node.isLeaf()) {
            node = !ibs.readBit() ? node.left : node.right;
        }
        return node.data;
    }

    public void encode(Obstream obs) {
        int maxLen = 0;
        for (String codeword : this.codewords) {
            if (codeword.length() > maxLen) {
                maxLen = codeword.length();
            }
        }
        maxLen = (int) (Math.log(maxLen) / Math.log(2.0)) + 1;
        obs.writeInt(maxLen, 8);
        for (String codeword : this.codewords) {
            obs.writeInt(codeword.length(), maxLen);
        }
    }

}
