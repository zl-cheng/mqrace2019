package io.openmessaging;

import com.sun.corba.se.impl.orbutil.concurrent.ReentrantMutex;
import io.openmessaging.data.DataFile;
import io.openmessaging.util.LongResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManager {

//    public static AtomicLong time1 = new AtomicLong();
//    public static AtomicLong time2 = new AtomicLong();
//    private volatile long time = 0;

    private ConcurrentHashMap<Long, ThreadQueue> threadQueueMap;
    private volatile DataFile dataFile;
    private ConcurrentLinkedDeque<DataFile> dataFileQueue;
    private PriorityQueue<ThreadQueue> writePq;
    private Thread sortThread;
    private volatile boolean isWriting = false;

    private volatile boolean checkSortFinishFlag = false;
    private volatile boolean isSortFinishFlag = false;

    static class ThreadQueue{
        volatile Thread thread;
        Deque<Message> pre;
        Deque<Message> cur;
        AtomicInteger size;
        volatile boolean isParked = false;

        public ThreadQueue(){
            thread = Thread.currentThread();
            cur = new ConcurrentLinkedDeque<>();
            pre = new ConcurrentLinkedDeque<>();
            size = new AtomicInteger();
        }
    }

    public DataManager(){
        threadQueueMap = new ConcurrentHashMap<>();
        dataFileQueue = new ConcurrentLinkedDeque<>();
        for(int i = 0; i < CONSTANCE.FILE_NUM; i++){
            try {
                dataFileQueue.addLast(new DataFile(CONSTANCE.srcPath, i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        dataFile = dataFileQueue.pollFirst();
        writePq = new PriorityQueue<>((n1, n2) -> {
            if(n1.pre.isEmpty())
                return -1;
            else if(n2.pre.isEmpty())
                return 1;
            return (int)n1.pre.peekFirst().getT() - (int)n2.pre.peekFirst().getT();
        });
        sortThread = new Thread(new SortThread());
        sortThread.setPriority(Thread.MAX_PRIORITY);
        sortThread.start();
    }

    public void put(Message message){
        long threadId = Thread.currentThread().getId();
        if(!threadQueueMap.containsKey(threadId)){
            synchronized (this){
                if(!threadQueueMap.containsKey(threadId)){
                    threadQueueMap.put(threadId, new ThreadQueue());
                }
            }
        }

        ThreadQueue threadQueue = threadQueueMap.get(threadId);
        while(isWriting || threadQueue.size.get() > CONSTANCE.WRITE_BUFFER_SIZE){
            threadQueue.isParked = true;
            LockSupport.park();
        }
        threadQueue.cur.addLast(message);
        threadQueue.size.incrementAndGet();
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) throws IOException, ExecutionException, InterruptedException {
        clearWriteBuffer();
        List<Message> list = new ArrayList<>();
        for(DataFile dataFile : dataFileQueue){
            dataFile.getMessage(list, aMin, aMax, tMin, tMax);
        }
        list.sort(Message.SORT_WITH_T);
        return list;
    }

    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) throws IOException {
        LongResult res = new LongResult();
        for(DataFile dataFile : dataFileQueue){
            dataFile.getAvgValue(res, aMin, aMax, tMin, tMax);
        }
        return res.getVal();
    }

    private void writeBuffer() throws InterruptedException, ExecutionException {
        for(ThreadQueue threadQueue : threadQueueMap.values()){
            if(!checkSortFinishFlag && threadQueue.thread.isAlive() && !threadQueue.isParked)
                return;
        }
        isWriting = true;
        writePq.clear();
        long min = Long.MAX_VALUE;
        for(ThreadQueue threadQueue : threadQueueMap.values()){
            if(threadQueue.size.get() != 0)
                min = Math.min(min, threadQueue.cur.peekLast().getT());
        }
        for(ThreadQueue threadQueue : threadQueueMap.values()){
            if(!threadQueue.thread.isAlive() && threadQueue.pre.isEmpty() && threadQueue.size.get() == 0)
                continue;
            while(!threadQueue.cur.isEmpty() && (checkSortFinishFlag || threadQueue.cur.peekFirst().getT() <= min)){
                threadQueue.pre.addLast(threadQueue.cur.pollFirst());
                threadQueue.size.decrementAndGet();
            }
            writePq.add(threadQueue);
        }
        isWriting = false;
        wakeUpThread();
        if(checkSortFinishFlag)
            isSortFinishFlag = true;
        while(!writePq.isEmpty()){
            ThreadQueue threadQueue = writePq.poll();
            if(threadQueue.pre.isEmpty()) {
                if(checkSortFinishFlag)
                    continue;
                return;
            }
            if(!checkSortFinishFlag && !writePq.isEmpty() && !writePq.peek().pre.isEmpty()){
                min = writePq.peek().pre.peek().getT();
            }else{
                min = threadQueue.pre.peek().getT();
            }
            while(!threadQueue.pre.isEmpty() && threadQueue.pre.peek().getT() <= min){
                if(dataFile.checkBlockFull()) {
                    dataFileQueue.addLast(dataFile);
                    dataFile = dataFileQueue.pollFirst();
                    if(dataFile.writeFuture != null)
                        dataFile.writeFuture.get();
                }
                dataFile.put(threadQueue.pre.poll());
//                threadQueue.size.decrementAndGet();
            }
            writePq.add(threadQueue);
        }
        wakeUpThread();
    }

    private void wakeUpThread(){
        for(ThreadQueue threadQueue : threadQueueMap.values()){
            if(threadQueue.isParked) {
                threadQueue.isParked = false;
                LockSupport.unpark(threadQueue.thread);
            }
        }
    }

    private void clearWriteBuffer() throws IOException {
        if(dataFile != null){
            synchronized (this){
                if(dataFile != null){
                    System.out.println("put end time:" + new Date());
                    checkSortFinishFlag = true;
                    try {
                        sortThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    dataFileQueue.add(dataFile);
                    for(DataFile file : dataFileQueue) {
                        file.syncCache();
                    }
                    dataFile = null;
                    threadQueueMap = null;
                    writePq = null;
                }
            }
        }
    }

    class SortThread extends Thread{
        @Override
        public void run() {
            while(threadQueueMap.size() < CONSTANCE.MAX_THREAD){
                Thread.yield();
            }
            while(true){
                if(checkSortFinishFlag && isSortFinishFlag)
                    return;
                try {
                    writeBuffer();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}