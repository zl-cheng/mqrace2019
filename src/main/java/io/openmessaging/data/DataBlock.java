package io.openmessaging.data;

import io.openmessaging.CONSTANCE;
import io.openmessaging.Message;
import io.openmessaging.cache.CachePool;
import io.openmessaging.util.LongResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 程智凌
 * @date 2019/8/1 20:09
 */
public class DataBlock {

    public volatile long indexStartPos;
    public volatile long metaStartPos;

//    public static AtomicInteger cnt1 = new AtomicInteger();
//    public static AtomicInteger cnt2 = new AtomicInteger();
//    public static AtomicInteger cnt3 = new AtomicInteger();
//    public static AtomicInteger cnt4 = new AtomicInteger();
//    public static AtomicInteger cnt5 = new AtomicInteger();
//    public static AtomicInteger cnt6 = new AtomicInteger();
//    public static AtomicInteger cnt7 = new AtomicInteger();

    private long minT = Long.MAX_VALUE;
    private long maxT = Long.MIN_VALUE;
    private long minA = Long.MAX_VALUE;
    private long maxA = Long.MIN_VALUE;
    public CachePool cachePool;
    public List<DataCell> cellList;
    public LongResult longResult;
    private ReentrantLock lock;

    public DataBlock(long indexStartPos, long metaStartPos, CachePool cachePool){
        this.indexStartPos = indexStartPos;
        this.metaStartPos = metaStartPos;
        this.cellList = new ArrayList<>();
        this.longResult = new LongResult();
        this.cachePool = cachePool;
        lock = new ReentrantLock();
    }

    public void put(Message message){
        minT = Math.min(minT, message.getT());
        maxT = Math.max(maxT, message.getT());
        minA = Math.min(minA, message.getA());
        maxA = Math.max(maxA, message.getA());
        longResult.add(message.getA());
    }

    public void getMessage(List<Message> list, FileChannel indexChannel, AsynchronousFileChannel metaChannel, long aMin, long aMax, long tMin, long tMax) throws IOException, ExecutionException, InterruptedException {
//        cnt1.incrementAndGet();
        int left = getLeft(aMin), right = getRight(aMax);
        ByteBuffer metaBuffer = cachePool.getWriteMetaBuffer();
        metaBuffer.clear();metaBuffer.limit((right -  left + 1) * CONSTANCE.META_SIZE * CONSTANCE.CEIL_SIZE);
        Future<Integer> metaFuture = metaChannel.read(metaBuffer, cellList.get(left).metaStartPos);
        ByteBuffer indexBuffer = cachePool.getIndexBufferInCache(this);
        boolean isInCache = false;
        int offset = 0;
        if(indexBuffer != null){
            isInCache = true;
            offset = (int)(cellList.get(left).indexStartPos - indexStartPos);
//            cnt6.incrementAndGet();
        }else{
            indexBuffer = cachePool.getWriteIndexBuffer();
            indexBuffer.clear();
            indexBuffer.limit((int)(cellList.get(right).indexStartPos + cellList.get(right).compressSizeA
                    + cellList.get(right).compressSizeT - cellList.get(left).indexStartPos));
            indexChannel.read(indexBuffer, cellList.get(left).indexStartPos);
            indexBuffer.flip();
        }
        metaFuture.get();
        metaBuffer.flip();
        DataCell dataCell;
        for(int i = left; i <= right; i++){
            dataCell = cellList.get(i);
            if(dataCell.getMaxA() < aMin || dataCell.getMinA() > aMax
                    || dataCell.getMaxT() < tMin || dataCell.getMinT() > tMax)
                continue;
//            cnt5.incrementAndGet();
            dataCell.selectAll(list, indexBuffer, metaBuffer,
                    cellList.get(left).indexStartPos - offset, cellList.get(left).metaStartPos, aMin, aMax, tMin, tMax);
        }
        if(!isInCache){
            cachePool.collectWriteIndexBuffer(indexBuffer);
        }
        cachePool.collectWriteMetaBuffer(metaBuffer);
    }

    public void getAver(LongResult result, FileChannel indexChannel, long aMin, long aMax, long tMin, long tMax)
            throws IOException {
        //二分法获取cell范围
        int left = getLeft(aMin), right = getRight(aMax);
        //尝试从缓存获取本block的ByteBuffer
        ByteBuffer buffer = cachePool.getIndexBufferInCache(this);
        if(buffer != null){
            //缓存命中
            getAverByBuffer(result, buffer, cellList.get(left).indexStartPos - indexStartPos
                    , left, right, aMin, aMax, tMin, tMax, true);
        }else{
            //缓存未命中
            //pass操作
            List<int[]> gaps = getGaps(result, left, right, aMin, aMax, tMin, tMax);
            //对象池循环使用ByteBuffer
            buffer = cachePool.getWriteIndexBuffer();
            for(int[] gap : gaps){
                buffer.clear();buffer.limit((int)(cellList.get(gap[1]).indexStartPos
                        + cellList.get(gap[1]).compressSizeT
                        + cellList.get(gap[1]).compressSizeA
                        - cellList.get(gap[0]).indexStartPos));
                indexChannel.read(buffer, cellList.get(gap[0]).indexStartPos);
                getAverByBuffer(result, buffer, 0, gap[0], gap[1]
                        , aMin, aMax, tMin, tMax, false);
            }
            //返还ByteBuffer
            cachePool.collectWriteIndexBuffer(buffer);
        }
    }

    private List<int[]> getGaps(LongResult result, int left, int right, long aMin, long aMax, long tMin, long tMax){
        DataCell dataCell;
        List<int[]> res = cachePool.getCollectCellList()[0];
        res.clear();
        boolean hasLeft = false;
        int pre = left;
        List<int[]> tmp = cachePool.getCollectCellList()[1];
        tmp.clear();
        for(int i = left; i <= right; i++){
            dataCell = cellList.get(i);
            if(dataCell.getMaxA() < aMin || dataCell.getMinA() > aMax
                    || dataCell.getMaxT() < tMin || dataCell.getMinT() > tMax) {
                if(hasLeft){
                    tmp.add(new int[]{pre, i - 1});
                    hasLeft = false;
                }
            }else if(dataCell.getMinA() >= aMin && dataCell.getMaxA() <= aMax
                    && dataCell.getMinT() >= tMin && dataCell.getMaxT() <= tMax) {
//                cnt3.incrementAndGet();
                if(hasLeft){
                    tmp.add(new int[]{pre, i -1});
                    hasLeft = false;
                }
                result.add(dataCell.getLongResult());
            }else{
                if(!hasLeft){
                    hasLeft = true;
                    pre = i;
                }
            }
        }
        if(hasLeft){
            tmp.add(new int[]{pre, right});
        }
        for(int i = 0; i < tmp.size(); i++){
            int[] arr = tmp.get(i);
            if(arr[1] >= arr[0] + CONSTANCE.COLLECT_CELL_SIZE){
                res.add(arr);
                continue;
            }
            for(int j = i + 1; j < tmp.size() && tmp.get(j)[0] < arr[0] + CONSTANCE.COLLECT_CELL_SIZE; j ++){
                i ++;
            }
            res.add(new int[]{arr[0], tmp.get(i)[1]});
        }
        return res;
    }

    private void getAverByBuffer(LongResult result, ByteBuffer buffer, long offset, int left, int right
            , long aMin, long aMax, long tMin, long tMax, boolean isPassed){
        DataCell dataCell;
        for(int i = left; i <= right; i++){
            dataCell = cellList.get(i);
            if(dataCell.getMaxA() < aMin || dataCell.getMinA() > aMax
                    || dataCell.getMaxT() < tMin || dataCell.getMinT() > tMax)
                continue;
            else if(dataCell.getMinA() >= aMin && dataCell.getMaxA() <= aMax
                    && dataCell.getMinT() >= tMin && dataCell.getMaxT() <= tMax) {
                if(isPassed)
                    result.add(dataCell.getLongResult());
            }else
                cellList.get(i).selectAllWithAver(result, buffer, cellList.get(left).indexStartPos - offset, aMin, aMax, tMin, tMax);
        }
    }

    public long getMinT(){
        return minT;
    }

    public long getMaxT(){
        return maxT;
    }

    public long getMinA(){
        return minA;
    }

    public long getMaxA(){
        return maxA;
    }

    public LongResult getLongResult(){
        return longResult;
    }

    public void printCeilInfo(){
        for(int i = 0; i < cellList.size(); i++){
            System.out.printf("index:%d ceil. aMin:%d, aMax:%d, tMin:%d, tMax:%d\n",
                   i , cellList.get(i).getMinA(), cellList.get(i).getMaxA()
                    , cellList.get(i).getMinT(), cellList.get(i).getMaxT());
        }
    }

    public int getLeft(long min){
        int left = 0, right = cellList.size() - 1, mid = 0;
        while(left < right){
            mid = left + (right - left) / 2;
            if(cellList.get(mid).getMinA() < min){
                if(mid == left)
                    break;
                left = mid;
            }else{
                right = mid - 1;
            }
        }
        return left;
    }

    public int getRight(long max){
        int left = 0, right = cellList.size() - 1, mid = 0;
        while(left < right){
            mid = left + (right - left) / 2;
            if(cellList.get(mid).getMaxA() <= max){
                left = mid + 1;
            }else{
                if(mid == right)
                    break;
                right = mid;
            }
        }
        return right;
    }
}
