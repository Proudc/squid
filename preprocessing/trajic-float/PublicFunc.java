import java.util.ArrayList;
import java.util.Calendar;

public class PublicFunc {
    
    public static String fileBaseName(String fileName) {
        int pos = fileName.lastIndexOf(".");
        if (pos == -1) {
            return fileName;
        }
        return fileName.substring(0, pos);
    }

    public static String fileExt(String fileName) {
        int pos = fileName.lastIndexOf(".");
        if (pos == -1) {
            return fileName;
        }
        return fileName.substring(pos + 1, fileName.length());
    }

    public static long doubleToLong(double value) {
        return bytesToLong(doubleToBytes(value));
    }

    public static double longToDouble(long value) {
        return bytesToDouble(longToBytes(value));
    }

    public static byte[] doubleToBytes(double value) {
        long valueInt = Double.doubleToRawLongBits(value);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[7 - i] = (byte) ((valueInt >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    public static byte[] floatToBytes(float value) {
        int valueInt = Float.floatToRawIntBits(value);
        byte[] byteRet = new byte[4];
        for (int i = 0; i < 4; i++) {
            byteRet[3 - i] = (byte) ((valueInt >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    public static double bytesToDouble(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static byte[] longToBytes(long num) {
        byte[] byteNum = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            byteNum[i] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static long bytesToLong(byte[] byteNum) {
        long num = 0;
        for (int i = 0; i < 8; i++) {
            num <<= 8;
            num |= (byteNum[i] & 0xff);
        }
        return num;
    }

    public static int convertTime(String date, String time, String originalDate, String originaTime) {
        String[] dates = date.split("-");
        // String[] dates = date.split("/");
        String[] times = time.split(":");
        String[] originalDates = originalDate.split("-");
        // String[] originalDates = originalDate.split("/");
        String[] originalTimes = originaTime.split(":");
        Calendar nowTime = Calendar.getInstance();
        Calendar originalTime = Calendar.getInstance();
        nowTime.set(Integer.valueOf(dates[0]), Integer.valueOf(dates[1]), Integer.valueOf(dates[2]),
                Integer.valueOf(times[0]), Integer.valueOf(times[1]), Integer.valueOf(times[2]));
        originalTime.set(Integer.valueOf(originalDates[0]), Integer.valueOf(originalDates[1]),
                Integer.valueOf(originalDates[2]), Integer.valueOf(originalTimes[0]), Integer.valueOf(originalTimes[1]),
                Integer.valueOf(originalTimes[2]));
        long nowSeconds = nowTime.getTimeInMillis() / 1000;
        long originalSeconds = originalTime.getTimeInMillis() / 1000;
        return (int) ((nowSeconds - originalSeconds) / 5);
    }


}
