package ca.rdmss.disruptor;

import java.util.concurrent.atomic.AtomicReference;

import com.lmax.disruptor.RingBuffer;

public class LMaxProducer<T> {

	final private RingBuffer<AtomicReference<T>> ringBuffer;
	
	public LMaxProducer(RingBuffer<AtomicReference<T>> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	public long publish(T t) {
		long sequence = ringBuffer.next();
		try {
			AtomicReference<T> wrapper = ringBuffer.get(sequence);
			wrapper.set(t);
		} finally {
			ringBuffer.publish(sequence);
		}
		return sequence;
	}

}
