package edu.usc.kmilner.mpj.taskDispatch.examples;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import edu.usc.kmilner.mpj.taskDispatch.MPJTaskCalculator;

/**
 * Simple {@link MPJTaskCalculator} example which sums the square of each task index.
 * 
 * @author Kevin Milner
 *
 */
public class MPJ_ThreadedAddTest extends MPJ_AddTest {
	
	private ExecutorService exec;

	public MPJ_ThreadedAddTest(CommandLine cmd) {
		super(cmd);
		
		// uses parsed --num-threads command line argument,
		// or Runtime.getRuntime().availableProcessors() if omitted
		exec = Executors.newFixedThreadPool(getNumThreads());
	}

	@Override
	protected void calculateBatch(int[] batch) throws Exception {
		// this does the actual calculation for a batch.
		
		ArrayList<Future<Double>> futures = new ArrayList<>();
		
		for (int i=0; i<batch.length; i++) {
			// submit new calculation
			int taskIndex = batch[i];
			AddCallable task = new AddCallable(taskIndex);
			futures.add(exec.submit(task));
		}
		
		// now wait on all futures and sum results
		for (Future<Double> future : futures)
			sum += future.get();
	}
	
	private class AddCallable implements Callable<Double> {
		
		private int taskIndex;

		public AddCallable(int taskIndex) {
			this.taskIndex = taskIndex;
		}

		@Override
		public Double call() throws Exception {
			Thread.sleep(sleepDuration);
			return (double)(taskIndex * taskIndex);
		}
		
	}
	
	@Override
	protected void doFinalAssembly() throws Exception {
		// shutdown executor
		exec.shutdown();
		// do assembly from parent class
		super.doFinalAssembly();
	}

	public static void main(String[] args) {
		args = MPJTaskCalculator.initMPJ(args);
		
		try {
			Options options = createOptions();
			
			CommandLine cmd = parse(options, args, MPJ_ThreadedAddTest.class);
			
			MPJ_ThreadedAddTest driver = new MPJ_ThreadedAddTest(cmd);
			driver.run();
			
			finalizeMPJ();
			
			System.exit(0);
		} catch (Throwable t) {
			abortAndExit(t);
		}
	}

}
