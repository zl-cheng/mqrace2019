package io.openmessaging;

import io.openmessaging.cache.CachePool;
import io.openmessaging.data.DataBlock;
import io.openmessaging.data.DataCell;
import io.openmessaging.data.DataStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore{

    private DataManager dataManager = new DataManager();

    private long time = 0;
    private AtomicInteger cnt1 = new AtomicInteger();
    private AtomicInteger cnt2 = new AtomicInteger();
    private AtomicInteger cnt3 = new AtomicInteger();

    @Override
    void put(Message message) {
        if(cnt1.incrementAndGet() % 10000000 == 0) {
            System.out.printf("%s  put time:%d, tSize:%d, aSize:%d, heap in:%d, heap out:%d\n",
                    new Date(), cnt1.get(), DataCell.tSize.get(), DataCell.aSize.get()
                    , CachePool.heapInSum.get(), CachePool.heapOutSum.get());
//            DataStatic.printJVMStatus();
        }
        dataManager.put(message);
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
//        if(cnt2.incrementAndGet() % 1000 == 0){
//            System.out.printf("%s get message time:%d, valid ceil:%d, block:%d, bingo:%d\n",
//                    new Date(), cnt2.get(), DataBlock.cnt5.get(), DataBlock.cnt1.get(),  DataBlock.cnt6.get());
//            DataStatic.printJVMStatus();
//        }
        try {
            return dataManager.getMessage(aMin, aMax, tMin, tMax);
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        if(cnt3.get() == 0)
//            time = System.currentTimeMillis();
//        if(cnt3.incrementAndGet() % 1000 == 0){
//            System.out.printf("get time:%d, cost time:%d\n",
//                    cnt3.get(), System.currentTimeMillis() - time);
//        }
        try {
            return dataManager.getAvgValue(aMin, aMax, tMin, tMax);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
