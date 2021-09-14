package src.main.scala.util.trajic;

public class LengthFrequencyDivider {
    
    public double[] freqs;
    public int nFreqs;
    public int maxDivs;
    public boolean forceMax;
    public double[][] costs;
    public int[][] path;

    public LengthFrequencyDivider(double[] freqs, int nFreqs, int maxDivs) {
        this(freqs, nFreqs, maxDivs, false);
    }

    public LengthFrequencyDivider(double[] freqs, int nFreqs, int maxDivs, boolean forceMax) {
        this.freqs    = freqs;
        this.nFreqs   = nFreqs;
        this.maxDivs  = maxDivs;
        this.forceMax = forceMax;
        this.costs    = new double[nFreqs][maxDivs];
        this.path     = new int[nFreqs][maxDivs];
    }

    public void calculate() {
        for (int i = 0; i < this.nFreqs; i++) {
            this.costs[i][0] = 0;
            for (int j = 0; j < i; j++) {
                this.costs[i][0] += (i - j) * this.freqs[j];
            }
        }

        for (int j = 1; j < this.maxDivs; j++) {
            this.costs[0][j] = 0;
        }

        for (int i = 1; i < this.nFreqs; i++) {
            for (int j = 1; j < this.maxDivs; j++) {
                this.costs[i][j] = Double.MAX_VALUE;

                for (int x = j - 1; x < i; x++) {
                    double c = this.costs[x][j - 1];
                    for (int y = x + 1; y < i; y++) {
                        c += (i - y) * this.freqs[y];
                    }
                    if (c < this.costs[i][j]) {
                        this.costs[i][j] = c;
                        this.path[i][j] = x;
                    }
                }
            }
        }
    }

    public int lastDiv(int nDivs) {
        int x = this.nFreqs - 1;
        if (!this.forceMax) {
            while(x > nDivs && this.freqs[x] == 0) {
                x--;
            }
        }
        return x;
    }

    public void getDividers(int[] arr, int nDivs) {
        arr[nDivs - 1] = lastDiv(nDivs);
        for (int j = nDivs - 2; j >= 0; j--) {
            arr[j] = this.path[arr[j + 1]][j + 1];
        }
    }

    public double getCost(int nDivs) {
        return this.costs[lastDiv(nDivs)][nDivs - 1] + Math.log(nDivs) / Math.log(2.0);
    }
}
