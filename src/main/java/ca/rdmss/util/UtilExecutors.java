package ca.rdmss.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.util.Util;

import ca.rdmss.concurrent.ExecutorServiceWrapper;
import ca.rdmss.concurrent.LMaxBlockingQueue;
import ca.rdmss.concurrent.LMaxExecutor;
import ca.rdmss.concurrent.LMaxThreadPoolExecutor;
import ca.rdmss.concurrent.MyExecutorService;
import ca.rdmss.concurrent.MyThreadPoolExecutor;

public class UtilExecutors {

	
	public static long DEF_KEEP_ALIVE_TIME_MLS = 100L;
	public static int DEF_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	public static WaitStrategy DEF_WAIT_STRATEGY = new SleepingWaitStrategy();
	public static int DEF_BUFFER_SIZE = Util.ceilingNextPowerOfTwo(1024*16); // 16K
	
	public static boolean DO_REPORTER_WRAPPER = true;

    public static MyExecutorService newFixedThreadPool() {
        return newFixedThreadPool(DEF_POOL_SIZE);
    }

    public static MyExecutorService newFixedThreadPool(int nThreads) {
        return newThreadPoolExecutor(nThreads, new LinkedBlockingQueue<>());
    }

    public static MyExecutorService newFastThreadPool() {
        return newFastThreadPool(DEF_POOL_SIZE);
    }

    public static MyExecutorService newFastThreadPool(int nThreads) {
        return DO_REPORTER_WRAPPER? new ExecutorServiceWrapper(newThreadPoolExecutor(nThreads, new LMaxBlockingQueue<Runnable>(DEF_WAIT_STRATEGY)))
        		: newThreadPoolExecutor(nThreads, new LMaxBlockingQueue<Runnable>(DEF_WAIT_STRATEGY));
    }

    public static MyExecutorService newThreadPoolExecutor(int nThreads, BlockingQueue<Runnable> queue) {
        return DO_REPORTER_WRAPPER? new ExecutorServiceWrapper(new MyThreadPoolExecutor(nThreads, nThreads, DEF_KEEP_ALIVE_TIME_MLS, TimeUnit.MILLISECONDS, queue))
        		:new MyThreadPoolExecutor(nThreads, nThreads, DEF_KEEP_ALIVE_TIME_MLS, TimeUnit.MILLISECONDS, queue);
    }


    public static MyExecutorService newLMaxThreadPoolExecutor() {
        return newLMaxThreadPoolExecutor(DEF_POOL_SIZE, new SleepingWaitStrategy());
    }

    public static MyExecutorService newLMaxThreadPoolExecutor(int nThreads) {
        return newLMaxThreadPoolExecutor(nThreads, new SleepingWaitStrategy());
    }

    public static MyExecutorService newLMaxThreadPoolExecutor(int nThreads, WaitStrategy waitStrategy) {
        return DO_REPORTER_WRAPPER? new ExecutorServiceWrapper(new LMaxThreadPoolExecutor(nThreads, nThreads, DEF_KEEP_ALIVE_TIME_MLS, TimeUnit.MILLISECONDS,  
        			Executors.defaultThreadFactory(), waitStrategy, DEF_BUFFER_SIZE))
        		:new LMaxThreadPoolExecutor(nThreads, nThreads, DEF_KEEP_ALIVE_TIME_MLS, TimeUnit.MILLISECONDS,  
            			Executors.defaultThreadFactory(), waitStrategy, DEF_BUFFER_SIZE);
    }

	public static MyExecutorService newLMaxExecutor() {
		return DO_REPORTER_WRAPPER? new ExecutorServiceWrapper(new LMaxExecutor(new SleepingWaitStrategy(), DEF_BUFFER_SIZE))
				: new LMaxExecutor(new SleepingWaitStrategy(), DEF_BUFFER_SIZE);
	}
}
