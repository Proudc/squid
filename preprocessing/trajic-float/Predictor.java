public abstract class Predictor {
    
    public abstract float predictTime(float[][] tuples, int index);

    public abstract void predictCoords(float[][] tuples, int index, float[] result);

    public abstract float predictTime(int[][] tuples, int index);

    public abstract void predictCoords(int[][] tuples, int index, float[] result);

    public abstract void predictCoordsNoTime(float[][] tuples, int index, float[] result);

    public abstract void predictCoordsNoTime(int[][] tuples, int index, float[] result);

}
