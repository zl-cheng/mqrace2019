package io.openmessaging.cache;

import io.openmessaging.CONSTANCE;
import io.openmessaging.data.DataBlock;
import io.openmessaging.data.DataCell;
import io.openmessaging.util.CompressUtil2;
import io.openmessaging.util.UnsignedDeltaCompress;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 程智凌
 * @date 2019/8/1 20:07
 */
public class CachePool{
    public static AtomicLong heapInSum = new AtomicLong();
    public static AtomicLong heapOutSum = new AtomicLong();

    private ThreadLocal<byte[][]> compressArr;
    private ThreadLocal<long[][]> decompressArr;
    private Queue<ByteBuffer> indexWriteBufferQueue;
    private Queue<ByteBuffer> metaWriteBufferQueue;
    private ThreadLocal<List<int[]>[]> collectCellList;
    private AtomicLong heapInSize = new AtomicLong();
    private AtomicLong heapOutSize = new AtomicLong();
    private ConcurrentHashMap<DataBlock, ByteBuffer> cacheBlockMap;
    private ThreadLocalRandom random;

    public CachePool(){
        random = ThreadLocalRandom.current();
        compressArr = new ThreadLocal<>();
        decompressArr = new ThreadLocal<>();
        collectCellList = new ThreadLocal<>();
        indexWriteBufferQueue = new ConcurrentLinkedQueue<>();
        metaWriteBufferQueue = new ConcurrentLinkedQueue<>();
        cacheBlockMap = new ConcurrentHashMap<>();
        for(int i = 0; i < CONSTANCE.MAX_THREAD; i++)
            metaWriteBufferQueue.add(ByteBuffer.allocate(CONSTANCE.BLOCK_SIZE * CONSTANCE.META_SIZE));
        for(int i = 0; i < CONSTANCE.MAX_THREAD; i++)
            indexWriteBufferQueue.add(ByteBuffer.allocate(CONSTANCE.BLOCK_SIZE * CONSTANCE.INDEX_SIZE ));
    }

    public void clear(){
        compressArr = new ThreadLocal<>();
        decompressArr = new ThreadLocal<>();
    }

    public byte[][] getCompressBytes(){
        if(compressArr.get() == null){
            compressArr.set(new byte[2][CONSTANCE.CEIL_SIZE * 9]);
        }
        return compressArr.get();
    }

    public long[][] getDecompressBytes(){
        if(decompressArr.get() == null){
            decompressArr.set(new long[2][CONSTANCE.CEIL_SIZE ]);
        }
        return decompressArr.get();
    }

    public List<int[]>[] getCollectCellList(){
        if(collectCellList.get() == null){
            collectCellList.set(new List[]{new ArrayList<>(), new ArrayList<>()});
        }
        return collectCellList.get();
    }

    public ByteBuffer getWriteIndexBuffer(){
        return indexWriteBufferQueue.poll();
    }

    public void collectWriteIndexBuffer(ByteBuffer buffer){
        indexWriteBufferQueue.add(buffer);
    }

    public void tryCacheIndexBuffer(DataBlock block, ByteBuffer buffer){
        int val = random.nextInt(100), size = buffer.position();
        if(val < CONSTANCE.HEAP_CACHE_RATE && heapInSize.get() + size < CONSTANCE.HEAP_CACHE_SIZE){
            heapInSize.addAndGet(size);
            ByteBuffer saveBuffer = ByteBuffer.allocate(size);
            buffer.rewind();
            saveBuffer.put(buffer);
            cacheBlockMap.put(block, saveBuffer);
            heapInSum.addAndGet(size);
        }else if(val < (CONSTANCE.HEAP_CACHE_RATE + CONSTANCE.HEAP_OUT_CACHE_RATE)
                && heapOutSize.get() + size < CONSTANCE.HEAP_OUT_CACHE_SIZE){
            heapOutSize.addAndGet(size);
            ByteBuffer saveBuffer = ByteBuffer.allocateDirect(size);
            buffer.rewind();
            saveBuffer.put(buffer);
            cacheBlockMap.put(block, saveBuffer);
            heapOutSum.addAndGet(size);
        }
    }

    public ByteBuffer getIndexBufferInCache(DataBlock block){
        return cacheBlockMap.get(block);
    }

    public ByteBuffer getWriteMetaBuffer(){
        return metaWriteBufferQueue.poll();
    }

    public void collectWriteMetaBuffer(ByteBuffer buffer){
        metaWriteBufferQueue.add(buffer);
    }

    public long[] getReadIndexA(ByteBuffer buffer, DataCell dataCell, long base) {
        byte[] bytes = getCompressBytes()[0];
        long[] arrA = getDecompressBytes()[0];
        int st = (int)(dataCell.indexStartPos - base + dataCell.compressSizeT);
        for(int i = 0; i < dataCell.compressSizeA; i++){
            bytes[i] = buffer.get(st + i);
        }
        UnsignedDeltaCompress.bytes2long(bytes, arrA, dataCell.getMinA(), dataCell.size);
        return arrA;
    }

    public long[] getReadIndexT(ByteBuffer buffer, DataCell dataCell, long base){
        byte[] bytes = getCompressBytes()[1];
        long[] arrT = getDecompressBytes()[1];
        int st = (int)(dataCell.indexStartPos - base);
        for(int i = 0; i < dataCell.compressSizeT; i++){
            bytes[i] = buffer.get(st + i);
        }
        CompressUtil2.bytes2long(bytes, arrT, dataCell.compressStartT, dataCell.size);
        return arrT;
    }


}
