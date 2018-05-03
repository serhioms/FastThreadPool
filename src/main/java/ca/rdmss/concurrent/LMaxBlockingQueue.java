package ca.rdmss.concurrent;

import java.util.concurrent.LinkedBlockingQueue;

import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;

import ca.rdmss.disruptor.LMaxDisruptor;

public class LMaxBlockingQueue extends LinkedBlockingQueue<Runnable> {

	private static final long serialVersionUID = -1903203266046230649L;

	final private LMaxDisruptor<Runnable> disruptor;
	
	public LMaxBlockingQueue() {
		this(new SleepingWaitStrategy());
	}
	
	public LMaxBlockingQueue(WaitStrategy waitStrategy) {
		super();
		this.disruptor = new LMaxDisruptor<Runnable>(waitStrategy);

		disruptor.subscribeConsumer((event, sequence, endOfBatch)->{
			super.offer(event.get());
		});

		disruptor.start();
	}

	@Override
	public boolean offer(Runnable r) {
		return disruptor.publish(r);
	}

	@Override
	public void clear() {
		//disruptor.shutdown();
	}
}
