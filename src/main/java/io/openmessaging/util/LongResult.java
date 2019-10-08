package io.openmessaging.util;

/**
 * @author 程智凌
 * @date 2019/8/1 18:16
 */
public class LongResult {
//    int high = 0;
    long low = 0;
    int count = 0;

    public void add(long num){
        low += num;
        count ++;
    }

    public void add(LongResult num){
        low += num.low;
        count += num.count;
    }

    public long getVal(){
        if(count == 0)
            return 0;
        return low / count;
    }

}
