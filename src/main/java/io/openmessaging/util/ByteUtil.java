package io.openmessaging.util;

public class ByteUtil {

    public static int bytes2Int(byte[] arr, int st){
        return (arr[st + 3] & 0xff) | (arr[st + 2] & 0xff) << 8 | (arr[st + 1] & 0xff) << 16 | (arr[st] & 0xff) << 24;
    }

    public static void int2Bytes(int a, byte[] arr, int st){
        arr[st] = (byte)((a >> 24) & 0xFF);
        arr[st + 1] = (byte)((a >> 16) & 0xFF);
        arr[st + 2] = (byte)((a >> 8) & 0xFF);
        arr[st + 3] = (byte)(a & 0xFF);
    }

    public static long bytes2Long(byte[] arr, int st){
        return (arr[st + 7] & 255L) | (arr[st + 6] & 255L) << 8 | (arr[st + 5] & 255L) << 16 | (arr[st + 4] & 255L) << 24
            | (arr[st + 3] & 255L) << 32 | (arr[st + 2] & 255L) << 40 | (arr[st + 1] & 255L) << 48 | (arr[st] & 255L) << 56;
    }

    public static void long2Bytes(long a, byte[] arr, int st){
        arr[st + 0] = (byte)((a >> 56) & 0xFF);
        arr[st + 1] = (byte)((a >> 48) & 0xFF);
        arr[st + 2] = (byte)((a >> 40) & 0xFF);
        arr[st + 3] = (byte)((a >> 32) & 0xFF);
        arr[st + 4] = (byte)((a >> 24) & 0xFF);
        arr[st + 5] = (byte)((a >> 16) & 0xFF);
        arr[st + 6] = (byte)((a >> 8) & 0xFF);
        arr[st + 7] = (byte)(a & 0xFF);
    }

    public static void reverse(byte[] arr){
        byte tmp = 0;
         for(int i = 0, len = arr.length; i < len / 2; i++){
             tmp = arr[i];
             arr[i] = arr[len - 1 - i];
             arr[len - 1 - i] = tmp;
         }
    }

//    public static void main(String[] args) {
//        int val = 1000;
//        System.out.println("v: " + val + " bytes: " + Arrays.toString(ByteUtil.int2Bytes(val)));
//    }

}
