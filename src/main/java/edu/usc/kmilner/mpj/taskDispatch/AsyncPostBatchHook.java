package edu.usc.kmilner.mpj.taskDispatch;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Asynchronous post batch hook, does not block Dispatcher.
 * 
 * @author Kevin Milner
 *
 */
public abstract class AsyncPostBatchHook implements PostBatchHook {
	
	private ExecutorService exec;
	private List<Future<?>> futures;
	
	private int numFinished;
	private int numRunning;
	private int numQueued;
	
	private long millisSpent;
	
	public AsyncPostBatchHook(int threads) {
		Preconditions.checkState(threads > 0);
		
		if (threads == 1)
			exec = Executors.newSingleThreadExecutor();
		else
			exec = Executors.newFixedThreadPool(threads);
		
		futures = new LinkedList<Future<?>>();
	}
	
	public void batchProcessed(int[] batch, int processIndex) {
		ProcessHookRunnable run = new ProcessHookRunnable(batch, processIndex);
		numQueued += batch.length;
		futures.add(exec.submit(run));
	}
	
	private class ProcessHookRunnable implements Runnable {
		private int[] batch;
		private int processIndex;
		
		public ProcessHookRunnable(int[] batch, int processIndex) {
			this.batch = batch;
			this.processIndex = processIndex;
		}

		@Override
		public void run() {
			Stopwatch watch = Stopwatch.createStarted();
			numQueued -= batch.length;
			numRunning += batch.length;
			batchProcessedAsync(batch, processIndex);
			numRunning -= batch.length;
			numFinished += batch.length;
			millisSpent += watch.elapsed(TimeUnit.MILLISECONDS);
		}
	}
	
	public void shutdown() {
		exec.shutdown();
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (Exception e) {
				if (e instanceof RuntimeException)
					throw (RuntimeException)e;
				throw new RuntimeException(e);
			}
		}
	}
	
	protected abstract void batchProcessedAsync(int[] batch, int processIndex);

	/**
	 * @return the number of completed tasks (not the number of batches)
	 */
	public int getNumFinished() {
		return numFinished;
	}

	/**
	 * @return the number of running tasks in the current batch (or zero if no current processing)
	 */
	public int getNumRunning() {
		return numRunning;
	}

	/**
	 * @return the number of queued tasks for processing (across all batches)
	 */
	public int getNumQueued() {
		return numQueued;
	}
	
	/**
	 * @return String representation of running/queued/finished tasks
	 */
	public String getCountsString() {
		return "running="+getNumRunning()+", queued="+getNumQueued()+", finished="+getNumFinished();
	}
	
	public String getRatesString() {
		String str = "rate: ";
		if (getNumFinished() > 0) {
			str += Utils.smartRatePrint(getNumFinished(), millisSpent);
			double millisPerTask = (double)millisSpent/(double)getNumFinished();
			if (getNumRunning() > 0) {
				double millis = getNumRunning() * millisPerTask;
				str += ", time for running: ~"+Utils.smartTimePrint(millis);
			}
			if (getNumQueued() > 0) {
				double millis = getNumQueued() * millisPerTask;
				str += ", time for queue: ~"+Utils.smartTimePrint(millis);
			}
		} else {
			str += "n/a";
		}
		return str;
	}

}
