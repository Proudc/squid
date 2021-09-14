public abstract class Encoder {
    
    public abstract void encode(Obstream obs, int num);

    public abstract long decode(Ibstream ibs);

}
