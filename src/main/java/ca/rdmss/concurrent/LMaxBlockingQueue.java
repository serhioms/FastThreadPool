package ca.rdmss.concurrent;

import java.util.concurrent.LinkedBlockingQueue;

import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;

import ca.rdmss.disruptor.LMaxDisruptor;

public class LMaxBlockingQueue<T> extends LinkedBlockingQueue<T> {

	private static final long serialVersionUID = -1903203266046230649L;

	final private LMaxDisruptor<T> disruptor;
	
	public LMaxBlockingQueue() {
		this(new SleepingWaitStrategy());
	}
	
	public LMaxBlockingQueue(WaitStrategy waitStrategy) {
		super();
		this.disruptor = new LMaxDisruptor<T>(waitStrategy);

		disruptor.subscribeConsumer((event, sequence, endOfBatch)->{
			super.offer(event.get());
		});

		disruptor.start();
	}

	@Override
	public boolean offer(T r) {
		disruptor.publish(r);
		return true;
	}
}
