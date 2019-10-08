package io.openmessaging.util;

import java.util.List;

/**
 * @author 程智凌
 * @date 2019/9/2 14:16
 */
public class UnsignedDeltaCompress {
    public static int long2bytes(List<Long> list, byte[] arr){
        int pos = 0;
        long delta = 0;
        for(int i = 1; i < list.size(); i++){
            delta = list.get(i) - list.get(i - 1);
            if(delta == 0) {
                //0
                pos += 8;
            }else if(delta <= ((1L << 7) - 1)){
                //0
                pos ++;
                long2bits(arr, 7, pos, delta);
                pos += 7;
            }else if(delta <= ((1L << 14) - 1)){
                //10 14bit
                arr[pos >> 3] |= 0x01;
                pos += 2;
                long2bits(arr, 14, pos, delta);
                pos += 14;
            }else if(delta <= ((1L << 21) - 1)){
                //110 21bit
                arr[pos >> 3] |= 0x03;
                pos += 3;
                long2bits(arr, 21, pos, delta);
                pos += 21;
            }else if(delta <= ((1L << 28) - 1)){
                //1110 28bit
                arr[pos >> 3] |= 0x07;
                pos += 4;
                long2bits(arr, 28, pos, delta);
                pos += 28;
            }else if(delta <= ((1L << 35) - 1)){
                //1111 0 35bit
                arr[pos >> 3] |= 0x0f;
                pos += 5;
                long2bits(arr, 35, pos, delta);
                pos += 35;
            }else if(delta <= ((1L << 42) - 1)){
                //1111 10 42bit
                arr[pos >> 3] |= 0x1f;
                pos += 6;
                long2bits(arr, 42, pos, delta);
                pos += 42;
            }else if(delta <= ((1L << 49) - 1)){
                //1111 110 49bit
                arr[pos >> 3] |= 0x3f;
                pos += 7;
                long2bits(arr, 49, pos, delta);
                pos += 49;
            }else if(delta <= ((1L << 56) - 1)){
                //1111 1110 56bit
                arr[pos >> 3] |= 0x7f;
                pos += 8;
                long2bits(arr, 56, pos, delta);
                pos += 56;
            }else{
                //1111 1111 64bit
                arr[pos >> 3] |= 0xff;
                pos += 8;
                ByteUtil.long2Bytes(delta, arr, pos / 8);
//                long2bits(arr, 64, pos, delta);
                pos += 64;
            }
        }
        return (pos / 8 + 1);
    }

    public static void bytes2long(byte[] arr, long[] res, long pre, int len){
        res[0] = pre;
        int pos = 0;
        long delta = 0;
        for(int i = 1; i < len; i++){
            delta = 0;
            if(arr[pos] == 0) {
                pos ++;
            }else if((arr[pos] & 0x01) == 0x00){
                //0
                delta += ((arr[pos] & 0xfe) >> 1);
                pos ++;
            }else if((arr[pos] & 0x03) == 0x01){
                //10
                delta += ((arr[pos] & 0xfc) >> 2);
                delta += ((arr[pos + 1] & 0xff) << 6);
                pos += 2;
            }else if((arr[pos] & 0x07) == 0x03){
                //110
                delta += ((arr[pos] & 0xf8) >> 3);
                delta += ((arr[pos + 1] & 0xff) << 5);
                delta += ((arr[pos + 2] & 0xff) << 13);
                pos += 3;
            }else if((arr[pos] & 0x0f) == 0x07){
                //1110
                delta += ((arr[pos] & 0xf0) >> 4);
                delta += ((arr[pos + 1] & 0xff) << 4);
                delta += ((arr[pos + 2] & 0xff) << 12);
                delta += ((arr[pos + 3] & 0xff) << 20);
                pos += 4;
            }else if((arr[pos] & 0x1f) == 0x0f){
                //1111 0
                delta += ((arr[pos] & 0xe0) >> 5);
                delta += ((arr[pos + 1] & 0xff) << 3);
                delta += ((arr[pos + 2] & 0xff) << 11);
                delta += ((arr[pos + 3] & 0xff) << 19);
                delta += ((long)((arr[pos + 4] & 0xff)) << 27);
                pos += 5;
            }else if((arr[pos] & 0x3f) == 0x1f){
                //1111 10
                delta += ((arr[pos] & 0xc0) >> 6);
                delta += ((arr[pos + 1] & 0xff) << 2);
                delta += ((arr[pos + 2] & 0xff) << 10);
                delta += ((arr[pos + 3] & 0xff) << 18);
                delta += ((long)(arr[pos + 4] & 0xff) << 26);
                delta += ((long)(arr[pos + 5] & 0xff) << 34);
                pos += 6;
            }else if((arr[pos] & 0x7f) == 0x3f){
                //1111 110
                delta += ((arr[pos] & 0x80) >> 7);
                delta += ((arr[pos + 1] & 0xff) << 1);
                delta += ((arr[pos + 2] & 0xff) << 9);
                delta += ((arr[pos + 3] & 0xff) << 17);
                delta += ((long)(arr[pos + 4] & 0xff) << 25);
                delta += ((long)(arr[pos + 5] & 0xff) << 33);
                delta += ((long)(arr[pos + 6] & 0xff) << 41);
                pos += 7;
            }else if((arr[pos] & 0xff) == 0x7f){
                //1111 1110
                delta += (arr[pos + 1] & 0xff);
                delta += ((arr[pos + 2] & 0xff) << 8);
                delta += ((arr[pos + 3] & 0xff) << 16);
                delta += ((long)(arr[pos + 4] & 0xff) << 24);
                delta += ((long)(arr[pos + 5] & 0xff) << 32);
                delta += ((long)(arr[pos + 6] & 0xff) << 40);
                delta += ((long)(arr[pos + 7] & 0xff) << 48);
                pos += 8;
            }else{
                //1111 1111
                delta = ByteUtil.bytes2Long(arr, pos + 1);
//                delta += ((arr[pos + 1] & 0xff));
//                delta += ((arr[pos + 2] & 0xff) << 8);
//                delta += ((arr[pos + 3] & 0xff) << 16);
//                delta += ((arr[pos + 4] & 0xff) << 24);
//                delta += ((arr[pos + 5] & 0xff) << 32);
//                delta += ((arr[pos + 6] & 0xff) << 40);
//                delta += ((arr[pos + 7] & 0xff) << 48);
//                delta += ((arr[pos + 8] & 0xff) << 56);
                pos += 9;
            }
            res[i] = res[i - 1] + delta;
        }
    }

    private static void long2bits(byte[] arr, int len, int start, long num){
        for(int i = start, deep = 0; i < len + start; i += deep) {
            deep = Math.min(8 - (i & 0x7), len + start - i);
            arr[(i >> 3)] |= ((num & ((1 << deep) - 1)) << (i & 0x7));
            num >>= deep;
        }
    }
}
