package ca.mss.rd.stat;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class AverageMooving {

	static public int DEFAULT_DEPTH = 2;

	protected final Queue<Integer> values = new ConcurrentLinkedQueue<Integer>();
	protected int depth;
	protected AtomicInteger sum = new AtomicInteger(0);
	
	public AverageMooving() {
		this(DEFAULT_DEPTH);
	}

	public AverageMooving(int depth) {
		setDepth(depth);
		clear();
	}

	public void clear(){
		values.clear();
		sum.set(0);
	}

	public final int size(){
		return values.size();
	}
	
	public final int getDepth() {
		return depth;
	}
	
	public final boolean isFull() {
		return values.size() == depth;
	}
	
	public final boolean isAvailable() {
		return values.size() > 0;
	}
	
	public void setDepth(int depth){
		if( depth < 1 )
			throw new RuntimeException("Mooving depth can't be less then one");
		this.depth = depth;
		for(int i=values.size(); i>depth; i--){
			sum.addAndGet(-values.poll());
		}
	}

	public void addValue(int val){
		if( values.size() < depth ){
			sum.addAndGet(val);
		} else {
			sum.addAndGet(val - values.poll());
		}
		values.add(val);
	}
	
	public final int getAverage(){
		int size = values.size();
		return size==0? 0: sum.get()/size;
	}
	
}
