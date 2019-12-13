package ca.rdmss.concurrent;

import java.util.concurrent.ExecutorService;

public interface MyExecutorService extends ExecutorService {
	
	public boolean isRunning();
	public String report();
	public int size();
	
}
