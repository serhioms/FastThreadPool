package ca.rdmss.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.WaitStrategy;

import ca.mss.rd.stat.AverageMooving;
import ca.rdmss.disruptor.LMaxDisruptor;

public class LMaxThreadPoolExecutor implements MyExecutorService {

	private static final String NOT_IMPLEMENTED_S = "Not implemented: %s";
	
	private static final boolean IS_DESRUPTOR = true;

	final ThreadFactory threadFactory;
	final int maximumPoolSize;
	final Set<Worker> threadPool;

	private LMaxBlockingQueue<Runnable> queue;
	private LMaxDisruptor<Runnable> disruptor;
	
	@Override
	public int size() {
		return maximumPoolSize;
	}
	
	public LMaxThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            ThreadFactory threadFactory,
            WaitStrategy waitStrategy,
            int buffersize) {
		
		this.threadFactory = threadFactory;
		this.maximumPoolSize = maximumPoolSize;
		this.threadPool = ConcurrentHashMap.newKeySet(maximumPoolSize);

		if( IS_DESRUPTOR ){
			this.disruptor = new LMaxDisruptor<Runnable>(waitStrategy, buffersize);
			this.disruptor.subscribeConsumer((event, sequence, endOfBatch)->{
				consuming.incrementAndGet();
				executeRunnable(event.get());
				consuming.decrementAndGet();
				consumed.incrementAndGet();
			});
	
			disruptor.start();
		} else {
			this.queue = new LMaxBlockingQueue<Runnable>(waitStrategy);
			Thread consumer = new Thread(()->{
				try {
					for(Runnable command=queue.take(); command != null; command=queue.take() ){
						consuming.incrementAndGet();
						executeRunnable(command);
						consuming.decrementAndGet();
						consumed.incrementAndGet();
					}
				} catch(InterruptedException e){
					throw new RuntimeException(e);
				}
			});
			consumer.setName("blocking-queue-consumer");
			consumer.start();
		}
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
	
	private void executeRunnable(Runnable command) {
		if( runningThreads.get() < maximumPoolSize ) {
			runningThreads.incrementAndGet();
			Worker first = new Worker();
			threadFactory.newThread(first).start();
			first.setCommand(command);
		} else {
			while( threadPool.isEmpty() ) {
//				try {
//					TimeUnit.NANOSECONDS.sleep(1);
//				} catch (InterruptedException e) {}
			}
			Worker next = threadPool.iterator().next();
			if( threadPool.remove(next) ) {
				next.setCommand(command);
			} else {
				throw new RuntimeException("Can't remove worker from the pool: "+next);
			}
		}
	}

	@Override
	public void execute(Runnable command) {
		if( isShutdown.get() ) {
			throw new RuntimeException("Executor shutdown!");
		}
		publishing.incrementAndGet();
		if( IS_DESRUPTOR ){
			disruptor.publish(command);
		} else {
			queue.offer(command);
		}
		publishing.decrementAndGet();
		published.incrementAndGet();
	}

	@Override
	public boolean isRunning() {
		if( IS_DESRUPTOR ){
			return threadPool.size() < maximumPoolSize || disruptor.isRunning();
		} else {
			return threadPool.size() < maximumPoolSize || !queue.isEmpty();
		}
	}

	class Worker implements Runnable {
		volatile Runnable command;
		volatile CyclicBarrier lock = new CyclicBarrier(2);

		public void setCommand(Runnable command) {
			this.command = command;
			try {
				lock.await();
			} catch (InterruptedException | BrokenBarrierException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			while( !isTerminated.get() ) {
//				if( command != null ){
					try {
						lock.await();
						int rns = running.incrementAndGet();
						runningAvg.addValue(rns);
						command.run();
						running.decrementAndGet();
						executed.incrementAndGet();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (BrokenBarrierException e) {
						e.printStackTrace();
					} finally {
						command = null;
						threadPool.add(this);
					}
//				} else {
//					try {
//						TimeUnit.NANOSECONDS.sleep(1);
//					} catch (InterruptedException e) {}
//				}
			}
		}
	}
	
	volatile AtomicInteger runningThreads = new AtomicInteger(0);
	volatile AtomicBoolean isShutdown = new AtomicBoolean(false);
	volatile AtomicBoolean isTerminated = new AtomicBoolean(false);
	

	@Override
	public void shutdown() {
		isShutdown.set(true);
	}

	@Override
	public List<Runnable> shutdownNow() {
		return new ArrayList<Runnable>();
	}

	@Override
	public boolean isShutdown() {
		return isShutdown.get();
	}

	@Override
	public boolean isTerminated() {
		return isTerminated.get();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			unit.sleep(timeout);
		} catch (InterruptedException e) {
			new InterruptedException(e.getMessage());
		} finally {
			isTerminated.set(true);
		}
		return isTerminated.get();
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

	
}
