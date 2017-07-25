package edu.usc.kmilner.mpj.taskDispatch.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import edu.usc.kmilner.mpj.taskDispatch.MPJTaskCalculator;
import mpi.MPI;

/**
 * Simple {@link MPJTaskCalculator} example which sums the square of each task index.
 * 
 * @author Kevin Milner
 *
 */
public class MPJ_AddTest extends MPJTaskCalculator {
	
	// num tasks
	private int num = 500;
	// duration in millis of each task
	private long sleepDuration = 100;
	
	// total sum of squares for this worker
	private long sum;
	
	public MPJ_AddTest(CommandLine cmd) {
		super(cmd);
		
		if (cmd.hasOption("num"))
			num = Integer.parseInt(cmd.getOptionValue("num"));
		
		if (cmd.hasOption("time"))
			sleepDuration = (long)(1000d*Double.parseDouble(cmd.getOptionValue("time")));
	}

	@Override
	protected int getNumTasks() {
		return num;
	}

	@Override
	protected void calculateBatch(int[] batch) throws Exception {
		// this does the actual calculation for a batch.
		
		// this simple example is not threaded, but this is where you would add threading within 
		// a single worker if desired, using getNumThreads() to determine the thread count
		for (int i=0; i<batch.length; i++) {
			int taskIndex = batch[i];
			Thread.sleep(sleepDuration);
			long myResult = taskIndex * taskIndex;
			sum += myResult;
		}
	}

	@Override
	protected void doFinalAssembly() throws Exception {
		// this is where you put any needed MPI code required to process results at the end of the calculation.
		// leave empty if no such processing is needed
		
		// for this example, we will return our sum back to the root node
		
		long[] my_result = { sum };
		
		long[] worker_results = null;
		if (rank == 0)
			worker_results = new long[size];
		MPI.COMM_WORLD.Gather(my_result, 0, 1, MPI.LONG, worker_results, 0, 1, MPI.LONG, 0);
		
		if (rank == 0) {
			// print result to console
			long tot_sum = 0;
			for (long sum : worker_results)
				tot_sum += sum;
			System.out.println("Total calculated sum: "+tot_sum);
			// calculate expected value
			long expected = 0;
			for (int i=0; i<getNumTasks(); i++)
				expected += i*i;
			System.out.println("Expected sum: "+expected);
		}
	}
	
	public static Options createOptions() {
		Options ops = MPJTaskCalculator.createOptions();
		
		Option numOp = new Option("n", "num", true, "Number of tasks");
		numOp.setRequired(false);
		ops.addOption(numOp);
		
		Option timeOp = new Option("ts", "time", true, "Time per task in seconds");
		timeOp.setRequired(false);
		ops.addOption(timeOp);
		
		return ops;
	}
	
	public static void main(String[] args) {
		args = MPJTaskCalculator.initMPJ(args);
		
		try {
			Options options = createOptions();
			
			CommandLine cmd = parse(options, args, MPJ_AddTest.class);
			
			MPJ_AddTest driver = new MPJ_AddTest(cmd);
			driver.run();
			
			finalizeMPJ();
			
			System.exit(0);
		} catch (Throwable t) {
			abortAndExit(t);
		}
	}

}
