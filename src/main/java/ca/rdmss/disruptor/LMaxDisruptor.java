package ca.rdmss.disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;

public class LMaxDisruptor<T> {

	static public int BUFFER_SIZE = 1024;

	protected Disruptor<AtomicReference<T>> disruptor;
	protected RingBuffer<AtomicReference<T>> ringBuffer;

	protected LMaxProducer<T> producer;

	ThreadFactory threadFactory = new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, "LMaxDisruptor");
		}
	};
	
	public LMaxDisruptor() {
		this(new SleepingWaitStrategy());
	}  

	public LMaxDisruptor(WaitStrategy waitStrategy) {
		disruptor = new Disruptor<AtomicReference<T>>(
				new EventFactory<AtomicReference<T>>() {
					@Override
					public AtomicReference<T> newInstance() {
						return new AtomicReference<T>();
					}
				},
				Util.ceilingNextPowerOfTwo(BUFFER_SIZE), // size of the ring buffer must be power of 2
				threadFactory, 							 // each disrupter runs in 1 thread
				ProducerType.MULTI,
				waitStrategy
				);

		ringBuffer = disruptor.getRingBuffer();
		producer = new LMaxProducer<T>(ringBuffer);
	}  

	/*
	 * Connect consumers
	 */
	@SuppressWarnings("unchecked")
	public void subscribeConsumer(EventHandler<AtomicReference<T>>... consumer){
		disruptor.handleEventsWith(consumer); // TODO: There are many other disrupter handlers... 
	}
	
	@SuppressWarnings("unchecked")
	public void subscribeConsumer(EventHandler<AtomicReference<T>> consumer){
		disruptor.handleEventsWith(consumer); // TODO: There are many other disrupter handlers... 
	}
	
	public void start(){
		disruptor.start();
	}

	public void shutdown() {
		disruptor.shutdown();
	}

	public void shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException {
		disruptor.shutdown(timeout, timeUnit);
	}

	public boolean publish(T t) {
		return producer.publish(t);
	}
}
