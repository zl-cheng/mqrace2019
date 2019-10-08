package io.openmessaging.data;

import io.openmessaging.CONSTANCE;
import io.openmessaging.Message;
import io.openmessaging.cache.CachePool;
import io.openmessaging.util.CompressUtil2;
import io.openmessaging.util.LongResult;
import io.openmessaging.util.UnsignedDeltaCompress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 程智凌
 * @date 2019/8/1 20:12
 */
public class DataCell {

    public static AtomicLong tSize = new AtomicLong();
    public static AtomicLong aSize = new AtomicLong();
    public static AtomicLong cnt1 = new AtomicLong();

    private LongResult longResult;          //平均值信息
    public  long indexStartPos;             //index文件（存放t和a）的偏移量
    public  long metaStartPos;              //meta文件（存放body）的偏移量
    private long minA = Long.MAX_VALUE;
    private long maxA = Long.MIN_VALUE;
    private long minT = Long.MAX_VALUE;
    private long maxT = Long.MIN_VALUE;
    public  long compressStartT;            //解压时t的起始值（a起始值是minA）
    public  int compressSizeA;              //压缩后a的大小
    public  int compressSizeT;              //压缩后t的大小
    public  int size;                       //消息数

    public  int compressSizeBody;
    private CachePool cachePool;
    public DataCell(long indexStartPos, long metaStartPos, CachePool cachePool){
        longResult = new LongResult();
        this.indexStartPos = indexStartPos;
        this.metaStartPos = metaStartPos;
        this.cachePool = cachePool;
    }

    public void add(Message message){
        minA = Math.min(minA, message.getA());
        maxA = Math.max(maxA, message.getA());
        minT = Math.min(minT, message.getT());
        maxT = Math.max(maxT, message.getT());
        longResult.add(message.getA());
    }

    public void compress(List<Long> listA, List<Long> listT, List<byte[]> listBody, ByteBuffer indexBuffer, ByteBuffer metaBuffer){
        if(listT.isEmpty())
            return;
        compressStartT = listT.get(0);
        size = listT.size();
        compressSizeBody = size * CONSTANCE.META_SIZE;
//        compressSizeA = size * Long.BYTES;
        byte[] bytesT = cachePool.getCompressBytes()[1];
        Arrays.fill(bytesT, (byte)0);
        compressSizeT = CompressUtil2.long2bytes(listT, bytesT);
        indexBuffer.put(bytesT, 0, compressSizeT);
//        cachePool.tryPutIntoHeap(this, bytesT, compressSizeT);
        tSize.addAndGet(compressSizeT);
        byte[] bytesA = cachePool.getCompressBytes()[0];
        Arrays.fill(bytesA, (byte)0);
        compressSizeA = UnsignedDeltaCompress.long2bytes(listA, bytesA);
        indexBuffer.put(bytesA, 0, compressSizeA);
        aSize.addAndGet(compressSizeA);
    }

    public void selectAll(List<Message> list, ByteBuffer indexBuffer, ByteBuffer metaBuffer, long indexBase, long metaBase
            , long aMin, long aMax, long tMin, long tMax) {
        long[] arrA = cachePool.getReadIndexA(indexBuffer, this, indexBase)
                , arrT = cachePool.getReadIndexT(indexBuffer, this, indexBase);
        int base = (int)(metaStartPos - metaBase);
        for(int i = 0; i < size; i++){
            long t = arrT[i], a = arrA[i];
            if(a < aMin || t < tMin || t > tMax || a > aMax)
                continue;
//            cnt1.incrementAndGet();
            metaBuffer.position(base + i * CONSTANCE.META_SIZE);
            byte[] body = new byte[CONSTANCE.META_SIZE];
            metaBuffer.get(body);
            list.add(new Message(a, t, body));
        }
    }

    public void selectAllWithAver(LongResult result, ByteBuffer buffer, long indexBase, long aMin, long aMax, long tMin, long tMax){
        long[] arrA = cachePool.getReadIndexA(buffer, this, indexBase)
                , arrT = cachePool.getReadIndexT(buffer, this, indexBase);
        for(int i = 0; i < size; i++){
            long t = arrT[i], a = arrA[i];
            if(a < aMin || t < tMin || t > tMax || a > aMax)
                continue;
            else
                result.add(a);
        }
    }

    public long getMinA(){
        return minA;
    }

    public long getMaxA(){
        return maxA;
    }

    public long getMinT(){
        return minT;
    }

    public long getMaxT(){
        return maxT;
    }

    public LongResult getLongResult(){
        return longResult;
    }

}
