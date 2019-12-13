package ca.rdmss.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import ca.mss.rd.stat.AverageMooving;

public class ExecutorServiceWrapper implements MyExecutorService {

	final private MyExecutorService executor;
	
	public ExecutorServiceWrapper(MyExecutorService threadPool) {
		this.executor = threadPool;
	}

	@Override
	public int size() {
		return executor.size();
	}

	@Override
	public boolean isRunning() {
		return executor.isRunning() ;
	}

	AtomicInteger published = new AtomicInteger(0);
	AtomicInteger executed = new AtomicInteger(0);
	AtomicInteger executedlast = new AtomicInteger(0);
	AtomicInteger running = new AtomicInteger(0);
	AverageMooving runningAvg = new AverageMooving(50);
	AtomicInteger publishing = new AtomicInteger(0);

	@Override
	public String report() {
		int publishingget = publishing.get(), 
				publishedget = published.get(), 
//				runningget = running.get(), 
				runningget = runningAvg.getAverage(), 
				executedget = executed.get(),
				executedlastget = executedlast.get(),
				speed = executedget - executedlastget;
			
			String result = String.format("publishing=%d, published=%,d, running=%d, executed=%,d, reminder = %,d, speed=%,d", publishingget, publishedget, runningget, executedget, publishedget - executedget, speed);
			executedlast.set(executedget);
			return result;
	}
	
	@Override
	public void execute(Runnable command) {
		publishing.incrementAndGet();
		executor.execute(()->{
			int rns = running.incrementAndGet();
			runningAvg.addValue(rns);
			
			command.run();

			running.decrementAndGet();
			executed.incrementAndGet();
		});
		publishing.decrementAndGet();
		published.incrementAndGet();
	}


	@Override
	public void shutdown() {
		// System.out.println("shutdown");
		executor.shutdown();	
	}

	@Override
	public List<Runnable> shutdownNow() {
		// System.out.println("shutdownNow");
		return executor.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		// System.out.println("isShutdown");
		return executor.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		// System.out.println("isTerminated");
		return executor.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		// System.out.println("awaitTermination");
		return executor.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return executor.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return executor.submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return executor.submit(task);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return executor.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return executor.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return executor.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return executor.invokeAny(tasks, timeout, unit);
	}
}
