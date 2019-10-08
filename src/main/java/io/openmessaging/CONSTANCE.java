package io.openmessaging;

public interface CONSTANCE {
    String srcPath = "/alidata1/race2019/data";
//    String srcPath = "D:/AliwareMatch/test_src";
//    String srcPath = "C:/test_src";

    long HEAP_CACHE_SIZE = (1L << 29) + (1L << 27);
    long HEAP_OUT_CACHE_SIZE = (1L << 29) - (1L << 25);
    int BLOCK_SIZE = (1 << 14);
    int CEIL_SIZE = (1 << 9);
    int FILE_NUM = 4;

    int HEAP_CACHE_RATE = 19;
    int HEAP_OUT_CACHE_RATE = 16;
    int INDEX_SIZE = 16;
    int META_SIZE = 34;
    int MAX_THREAD = 12;
    int COLLECT_CELL_SIZE = 7;
    int WRITE_BUFFER_SIZE = (1 << 10);
    String INDEX_FILE_NAME = "index";
    String META_FILE_NAME = "meta";
}
