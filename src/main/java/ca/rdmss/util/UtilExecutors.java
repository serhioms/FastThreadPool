package ca.rdmss.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;

import ca.rdmss.concurrent.LMaxBlockingQueue;

public class UtilExecutors {

	
	public static long DEF_KEEP_ALIVE_TIME_MLS = 100L;
	public static int DEF_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	public static WaitStrategy DEF_WAIT_STRATEGY = new SleepingWaitStrategy();

    public static ThreadPoolExecutor newFastThreadPool() {
        return newFastThreadPool(DEF_POOL_SIZE);
    }

    public static ThreadPoolExecutor newFastThreadPool(int nThreads) {
        return newThreadPoolExecutor(nThreads, new LMaxBlockingQueue(DEF_WAIT_STRATEGY));
    }

    public static ThreadPoolExecutor newThreadPoolExecutor(int nThreads, BlockingQueue<Runnable> queue) {
        return new ThreadPoolExecutor(nThreads, nThreads, DEF_KEEP_ALIVE_TIME_MLS, TimeUnit.MILLISECONDS, queue);
    }
}
