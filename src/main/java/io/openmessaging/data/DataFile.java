package io.openmessaging.data;

import io.openmessaging.CONSTANCE;
import io.openmessaging.Message;
import io.openmessaging.cache.CachePool;
import io.openmessaging.util.LongResult;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 程智凌
 * @date 2019/8/1 20:20
 */
public class DataFile {

    public static AtomicLong time1 = new AtomicLong();
    public static AtomicLong time2 = new AtomicLong();

    private String filePath;
    private Queue<FileChannel> indexChannelPool;
    private Queue<AsynchronousFileChannel> metaChannelPool;
    private FileChannel indexChannel;
    private FileChannel metaChannel;
    private ExecutorService writeThreadPool;
    private FlushTask flushTask;
    public volatile Future<long[]> writeFuture;
    private List<DataBlock> blockList;
    private volatile DataBlock block;
    private List<Message>[] cacheMessageList;
    private CachePool cachePool;
    private volatile long messageCnt;

    public DataFile(String srcPath, int index) throws IOException {
        filePath = srcPath + "/" + index + "_";
        File indexFile = new File(srcPath + "/" + index + "_" + CONSTANCE.INDEX_FILE_NAME);
        if(!indexFile.exists())
            indexFile.createNewFile();
        indexChannel = FileChannel.open(indexFile.toPath(),
                StandardOpenOption.WRITE);

        File metaFile = new File(srcPath + "/" + index + "_" + CONSTANCE.META_FILE_NAME );
        if(!metaFile.exists())
            metaFile.createNewFile();
        metaChannel = FileChannel.open(metaFile.toPath(),
                StandardOpenOption.WRITE);

        writeThreadPool = Executors.newSingleThreadExecutor();
        cachePool = new CachePool();
        cacheMessageList = new List[]{new ArrayList<>(CONSTANCE.BLOCK_SIZE), new ArrayList<>(CONSTANCE.BLOCK_SIZE)};
        blockList = new ArrayList<>();
        flushTask = new FlushTask(cachePool, indexChannel, metaChannel);
    }

    public void put(Message message){
//        block.put(message);
        cacheMessageList[blockList.size() & 1].add(message);
    }

    public void getMessage(List<Message> list, long aMin, long aMax, long tMin, long tMax) throws IOException, ExecutionException, InterruptedException {
        FileChannel tmpIndexChannel = indexChannelPool.poll();
        AsynchronousFileChannel tmpMetaChannel = metaChannelPool.poll();
        int left = getLeft(tMin), right = getRight(tMax);
        for(int i = left; i <= right; i++){
            if(blockList.get(i).getMinT() > tMax || blockList.get(i).getMaxT() < tMin)
                continue;
            else
                blockList.get(i).getMessage(list, tmpIndexChannel, tmpMetaChannel, aMin, aMax, tMin, tMax);
        }

        indexChannelPool.add(tmpIndexChannel);
        metaChannelPool.add(tmpMetaChannel);
    }

    public void getAvgValue(LongResult res, long aMin, long aMax, long tMin, long tMax) throws IOException {
        FileChannel tmpIndexChannel = indexChannelPool.poll();
        int left = getLeft(tMin), right = getRight(tMax);
        for(int i = left; i <= right; i++){
            if(blockList.get(i).getMinT() > tMax || blockList.get(i).getMaxT() < tMin)
                continue;
            else if(blockList.get(i).getMinT() >= tMin && blockList.get(i).getMaxT() <= tMax
                    && blockList.get(i).getMinA() >= aMin && blockList.get(i).getMaxA() <= aMax) {
                res.add(blockList.get(i).getLongResult());
            }else
                blockList.get(i).getAver(res, tmpIndexChannel, aMin, aMax, tMin, tMax);
        }
        indexChannelPool.add(tmpIndexChannel);
    }

    public void syncCache() throws IOException {
        try {
            block = new DataBlock(writeFuture.get()[0], writeFuture.get()[1], cachePool);
            flush(true);
            writeFuture.get();
            blockList.add(block);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        indexChannel.close();
        metaChannel.close();
        cacheMessageList = null;
        writeThreadPool = null;
        flushTask = null;
        cachePool.clear();
        indexChannelPool = new ConcurrentLinkedDeque<>();
        metaChannelPool = new ConcurrentLinkedDeque<>();
        for(int i = 0; i <= CONSTANCE.MAX_THREAD; i++){
            indexChannelPool.add(FileChannel.open(new File(filePath + CONSTANCE.INDEX_FILE_NAME).toPath(),
                    StandardOpenOption.READ));
            metaChannelPool.add(AsynchronousFileChannel.open(new File(filePath + CONSTANCE.META_FILE_NAME).toPath(),
                    StandardOpenOption.READ));
        }
        block = null;
    }

    public boolean checkBlockFull(){
        if(++messageCnt % CONSTANCE.BLOCK_SIZE == 0){
            try {
                if(writeFuture != null){
                    while(!writeFuture.isDone()){
                        Thread.yield();
                    }
                    block = new DataBlock(writeFuture.get()[0], writeFuture.get()[1], cachePool);
                }else
                    block = new DataBlock(0, 0, cachePool);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            flush(false);
//            if (blockList.size() % CONSTANCE.FLUSH_SIZE == 0) {
//                flush(true);
//            } else{
//                flush(false);
//            }
            blockList.add(block);
            return true;
        }
        return false;
    }

    public void flush(boolean isForce){
        flushTask.set(cacheMessageList[blockList.size() & 1], block, isForce);
        writeFuture = writeThreadPool.submit(flushTask);
    }

    public int getLeft(long min){
        int left = 0, right = blockList.size() - 1, mid = 0;
        while(left < right){
            mid = left + (right - left) / 2;
            if(blockList.get(mid).getMinT() < min){
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
        int left = 0, right = blockList.size() - 1, mid = 0;
        while(left < right){
            mid = left + (right - left) / 2;
            if(blockList.get(mid).getMaxT() <= max){
                left = mid + 1;
            }else{
                if(mid == right)
                    break;
                right = mid;
            }
        }
        return right;
    }

    static class FlushTask implements Callable<long[]>{

        private List<Message> list;
        private DataBlock block;
        private CachePool cachePool;
        private boolean isForce;
        private FileChannel indexChannel;
        private FileChannel metaChannel;
        private List<Long> listA;
        private List<Long> listT;
        private List<byte[]> listBody;

        public FlushTask(CachePool cachePool
                , FileChannel indexFileChannel, FileChannel metaFileChannel){
            listA = new ArrayList<>();
            listT = new ArrayList<>();
            listBody = new ArrayList<>();
            this.cachePool = cachePool;
            this.indexChannel = indexFileChannel;
            this.metaChannel = metaFileChannel;
        }

        public void set(List<Message> list, DataBlock block, boolean isForce){
            this.list = list;
            this.block = block;
            this.isForce = isForce;
        }

        @Override
        public long[] call() throws Exception {
            long indexPos =  block.indexStartPos, metaPos = block.metaStartPos;
            Collections.sort(list, Message.SORT_WITH_A);
            ByteBuffer indexBuffer = cachePool.getWriteIndexBuffer(), metaBuffer = cachePool.getWriteMetaBuffer();
            indexBuffer.clear();metaBuffer.clear();
            for(int i = 0, len = list.size(); i < len; i += CONSTANCE.CEIL_SIZE){
                DataCell dataCell = new DataCell(indexPos, metaPos, cachePool);
                block.cellList.add(dataCell);
                listT.clear();listA.clear();
                for(int j = i; j < i + CONSTANCE.CEIL_SIZE && j < len; j++){
                    Message message = list.get(j);
                    dataCell.add(message);
                    block.put(message);
                    listT.add(message.getT());
                    listA.add(message.getA());
//                    indexBuffer.putLong(message.getA());
                    metaBuffer.put(message.getBody());
                }
                dataCell.compress(listA, listT, listBody, indexBuffer, metaBuffer);
                indexPos += (dataCell.compressSizeA + dataCell.compressSizeT);
                metaPos += dataCell.compressSizeBody;
            }
            indexBuffer.flip();
            metaBuffer.flip();
            indexChannel.write(indexBuffer, block.indexStartPos);
            metaChannel.write(metaBuffer, block.metaStartPos);
//            System.out.printf("block size:%d, index:%d, meta:%d\n", blockList.size(), block.indexStartPos, block.metaStartPos);
            if(isForce) {
                indexChannel.force(false);
                metaChannel.force(false);
            }
            cachePool.tryCacheIndexBuffer(block, indexBuffer);
            cachePool.collectWriteIndexBuffer(indexBuffer);
            cachePool.collectWriteMetaBuffer(metaBuffer);
            list.clear();
            return new long[]{indexPos, metaPos};
        }
    }

}