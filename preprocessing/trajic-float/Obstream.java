import java.io.DataOutputStream;

public class Obstream {

    public static int totalBits = 0;
    public static int totalBytes = 0;
    
    public DataOutputStream os;

    public byte part;

    public long pos;

    public Obstream(DataOutputStream os) {
        this.os   = os;
        this.part = 0;
        this.pos  = 0;
    }

    public void writeInt(long val, int nBits) {
        totalBits += nBits;
        this.part |= ((val << this.pos) & 0xFF);
        val >>= 8 - pos;
        if (nBits >= 8 - this.pos) {
            try {
                this.os.writeByte(this.part);
                totalBytes += 1;
            } catch(Exception e) {
                e.printStackTrace();
            }
            nBits -= 8 - this.pos;
            for (int i = 0; i < (nBits / 8); i++) {
                try {
                    this.os.writeByte((byte) val & 0xFF);
                    totalBytes += 1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                val >>= 8;
            }
            this.pos = nBits % 8;
            this.part = (byte) (val & (0xFF >> (8 - this.pos)));
        } else {
            this.pos += nBits;
        }
    }

    public void writeDouble(double val) {
        this.writeInt(Double.doubleToLongBits(val), 64);
    }

    public void writeFloat(float val) {
        this.writeInt(Float.floatToIntBits(val), 32);
    }

    public void writeBit(boolean val) {
        this.writeInt(val ? 1 : 0, 1);
    }

    public void close() {
        if (this.pos > 0) {
            try {
                this.os.writeByte(this.part);
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.pos = 0;
        }
    }

    public int numOfBytesWritten() {
        return totalBytes;
    }


}
