package io.openmessaging;

import io.openmessaging.util.ByteUtil;
import io.openmessaging.util.CompressUtil2;
import io.openmessaging.util.UnsignedDeltaCompress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author 程智凌
 * @date 2019/8/1 14:13
 */
public class BytesTest {
    public static void main(String[] args) {
//        byte[] arr = new byte[]{0, 0, 0, 0, 0, -111, -106, -6};
//        System.out.println(Arrays.toString(arr));
//        System.out.println(ByteUtil.bytes2Long(arr, 0));

        Random random = new Random();
        List<Long> list = new ArrayList<>();
        list.add(0L);
        for(int i = 1; i < 10000000; i ++){
            list.add(list.get(i - 1) + random.nextInt(Integer.MAX_VALUE));
        }
        byte[] bytes = new byte[list.size() * 9];
        UnsignedDeltaCompress.long2bytes(list, bytes);
        long[] arr = new long[list.size()];
        UnsignedDeltaCompress.bytes2long(bytes, arr, list.get(0), list.size());
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) != arr[i])
                System.out.println("error...");
        }


//        long time = System.currentTimeMillis(), time1 = 0;
//        for(int i = 0; i < 8321; i++){
//            time1 = System.nanoTime();
//        }
//        System.out.println(System.currentTimeMillis() - time);

    }
}
