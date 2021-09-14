import java.io.DataInputStream;

public class Ibstream {
    
    public DataInputStream is;

    public byte part;

    public int pos;

    public Ibstream(DataInputStream is) {
        this.is   = is;
        this.part = 0;
        this.pos  = 8;
    }

    public boolean readBit() {
        if (this.pos > 7) {
            try {
                this.part = is.readByte();
            } catch(Exception e) {
                e.printStackTrace();
            }
            this.pos = 0;
        }
        return ((this.part >> this.pos++) & 1) == 1;
    }

    public int readByte() {
        int n = 0;
        for (int i = 0; i < 8; i++) {
            if (readBit()) {
                n |= 1 << i;
            }
        }
        return n;
    }

    public long readInt(int size) {
        long n = 0;
        for (int i = 0; i < size / 8; i++) {
            long b = readByte();
            n |= b << (i * 8);
        }
        for (int i = (size / 8) * 8; i < size; i++) {
            if (readBit()) {
                long num = 1;
                n |= num << i;
            }
        }
        return n;
    }

}
