package ca.rdmss.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.WaitStrategy;

import ca.mss.rd.stat.AverageMooving;
import ca.rdmss.disruptor.LMaxDisruptor;

public class LMaxExecutor implements MyExecutorService {

	private static final String NOT_IMPLEMENTED_S = "Not implemented: %s";
	
	private LMaxDisruptor<Runnable> disruptor;
	
	@Override
	public int size() {
		return 1;
	}
	
	public LMaxExecutor(WaitStrategy waitStrategy, int buffersize) {
		this.disruptor = new LMaxDisruptor<Runnable>(waitStrategy, buffersize);
		this.disruptor.subscribeConsumer((event, sequence, endOfBatch)->{
			consuming.incrementAndGet();
			running.incrementAndGet();
			event.get().run();
			running.decrementAndGet();
			consuming.decrementAndGet();
			consumed.incrementAndGet();
			executed.incrementAndGet();
		});

		disruptor.start();
	}
	
	@Override
	public void execute(Runnable command) {
		if( disruptor.isShutdown() ) {
			throw new RuntimeException("Executor shutdown!");
		}
		publishing.incrementAndGet();
		disruptor.publish(command);
		publishing.decrementAndGet();
		published.incrementAndGet();
	}

	@Override
	public boolean isRunning() {
		return disruptor.isRunning();
	}

	@Override
	public void shutdown() {
		disruptor.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return new ArrayList<Runnable>();
	}

	@Override
	public boolean isShutdown() {
		return disruptor.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return disruptor.isShutdown();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			if( !disruptor.isShutdown() ){
				disruptor.shutdown(timeout, unit);
			}
		} catch (com.lmax.disruptor.TimeoutException e) {
			throw new InterruptedException(e.getMessage());
		}
		return true;
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "submit"));
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "submit"));
	}

	@Override
	public Future<?> submit(Runnable task) {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "submit"));
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "invokeAll"));
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "invokeAll"));
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "invokeAny"));
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new RuntimeException(String.format(NOT_IMPLEMENTED_S, "invokeAny"));
	}

	public String report() {
		int publishingget = publishing.get(), 
				publishedget = published.get(), 
				consumingget = consuming.get(), 
				consumedget = consumed.get(), 
//				runningget = running.get(), 
				runningget = runningAvg.getAverage(), 
				executedget = executed.get(),
				executedlastget = executedlast.get(),
				speed = executedget - executedlastget;
			
			String result = String.format("publishing=%d, published=%,d, consuming=%d, consumed=%,d, running=%d, executed=%,d, reminder = %,d, speed=%,d", publishingget, publishedget, consumingget, consumedget, runningget, executedget, publishedget - executedget, speed);
			executedlast.set(executedget);
			return result;
	}	
	
	AtomicInteger published = new AtomicInteger(0);
	AtomicInteger consumed = new AtomicInteger(0);
	AtomicInteger executed = new AtomicInteger(0);
	AtomicInteger executedlast = new AtomicInteger(0);
	AtomicInteger running = new AtomicInteger(0);
	AverageMooving runningAvg = new AverageMooving(50);
	AtomicInteger publishing = new AtomicInteger(0);
	AtomicInteger consuming = new AtomicInteger(0);
}
