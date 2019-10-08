package io.openmessaging.data;

import io.openmessaging.Message;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 程智凌
 * @date 2019/8/5 14:14
 */
public class DataStatic {

    private final static Object lock = new Object();
    private static MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    private static ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private AtomicInteger[][] counts;

    public static void printJVMStatus(){
        System.out.printf("%s free memory:%d, max memory:%d\n", new Date()
                , Runtime.getRuntime().freeMemory(),  Runtime.getRuntime().maxMemory());
//        System.out.println(new Date());
//        System.out.println("heap memory: " + memoryMXBean.getHeapMemoryUsage());
//        System.out.println("non heap memory: " + memoryMXBean.getNonHeapMemoryUsage());
//        System.out.println("total memory " + Runtime.getRuntime().totalMemory());
//        System.out.println("free memory " + Runtime.getRuntime().freeMemory());
//        System.out.println("max memory " + Runtime.getRuntime().maxMemory());
    }

    public static void printSystemStatus(){
        System.out.println(new Date());
        System.out.println();
    }


}
