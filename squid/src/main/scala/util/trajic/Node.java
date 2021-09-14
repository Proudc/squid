package src.main.scala.util.trajic;

public class Node {
    
    public int data;

    public Node left;

    public Node right;

    public double freq;

    public Node() {
        this.left  = null;
        this.right = null;
    }

    public Node(int data, double freq) {
        this.data  = data;
        this.freq  = freq;
        this.left  = null;
        this.right = null;
    }

    public Node(Node left, Node right) {
        this.left  = left;
        this.right = right;
    }

    public boolean isLeaf() {
        return this.left == null && this.right == null;
    }

    public double getFrequency() {
        return isLeaf() ? this.freq : this.left.getFrequency() + this.right.getFrequency();
    }
}
