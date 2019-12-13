package ca.rdmss.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyThreadPoolExecutor extends ThreadPoolExecutor implements MyExecutorService {

    public MyThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue) {
    	super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}
    
	@Override
	public String report() {
		throw new RuntimeException("Not implemented!");
	}

	@Override
	public int size() {
		return super.getCorePoolSize();
	}

	@Override
	public boolean isRunning() {
		return !super.getQueue().isEmpty() || super.getActiveCount() > 0;
	}
}
