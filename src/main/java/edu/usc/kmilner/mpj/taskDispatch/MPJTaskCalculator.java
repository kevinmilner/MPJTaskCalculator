package edu.usc.kmilner.mpj.taskDispatch;

import static edu.usc.kmilner.mpj.taskDispatch.Utils.getClassNameWithoutPackage;
import static edu.usc.kmilner.mpj.taskDispatch.Utils.smartTimePrint;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.google.common.base.Joiner;

import mpi.MPI;

/**
 * Abstract class for executing a fixed set of independent tasks via MPJ
 * 
 * @author Kevin Milner
 *
 */
public abstract class MPJTaskCalculator {

	protected static final int TAG_READY_FOR_BATCH = 1;
	protected static final int TAG_NEW_BATCH_LENGH = 2;
	protected static final int TAG_NEW_BATCH = 3;

	public static final int MIN_DISPATCH_DEFAULT = 5;
	public static final int MAX_DISPATCH_DEFAULT = 100;

	public static final boolean D = true;
	public static final SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");

	/**
	 * Can be used to disable all MPJ related calls to run this as a pure java single node test.
	 * Enabled with mpj.disable=true java property
	 */
	protected static final boolean SINGLE_NODE_NO_MPJ = Boolean.parseBoolean(System.getProperty("mpj.disable", "false"));

	protected int rank;
	protected int size;
	private int minDispatch;
	private int maxDispatch;
	private int exactDispatch;
	private boolean rootDispatchOnly;
	private int numThreads;
	protected boolean shuffle = true;

	private int startIndex;
	private int endIndex;

	private DispatcherThread dispatcher;

	private static DeadlockDetectionThread deadlock;

	protected String hostname;

	protected PostBatchHook postBatchHook;
	
	private ScheduledExecutorService timeoutScheduler;

	public MPJTaskCalculator(CommandLine cmd) {
		int numThreads = Runtime.getRuntime().availableProcessors();
		int minDispatch = MIN_DISPATCH_DEFAULT;
		int maxDispatch = MAX_DISPATCH_DEFAULT;
		int exactDispatch = -1;
		int startIndex = -1;
		int endIndex = -1;
		boolean rootDispatchOnly = false;

		if (cmd.hasOption("threads"))
			numThreads = Integer.parseInt(cmd.getOptionValue("threads"));

		if (cmd.hasOption("min-dispatch"))
			minDispatch = Integer.parseInt(cmd.getOptionValue("min-dispatch"));

		if (cmd.hasOption("max-dispatch"))
			maxDispatch = Integer.parseInt(cmd.getOptionValue("max-dispatch"));

		if (cmd.hasOption("exact-dispatch"))
			exactDispatch = Integer.parseInt(cmd.getOptionValue("exact-dispatch"));

		if (cmd.hasOption("root-dispatch-only"))
			rootDispatchOnly = true;

		if (cmd.hasOption("deadlock")) {
			deadlock = new DeadlockDetectionThread(5000);
			deadlock.start();
		}

		if (cmd.hasOption("start-index"))
			startIndex = Integer.parseInt(cmd.getOptionValue("start-index"));

		if (cmd.hasOption("end-index"))
			endIndex = Integer.parseInt(cmd.getOptionValue("end-index"));
		
		LocalDateTime endTime = null;
		if (cmd.hasOption("end-time"))
			endTime = LocalDateTime.parse(cmd.getOptionValue("end-time"), DateTimeFormatter.ISO_LOCAL_DATE_TIME);

		init(numThreads, minDispatch, maxDispatch, exactDispatch, rootDispatchOnly, startIndex, endIndex, endTime);
	}

	public MPJTaskCalculator(int numThreads, int minDispatch, int maxDispatch, boolean rootDispatchOnly) {
		init(numThreads, minDispatch, maxDispatch, -1, rootDispatchOnly);
	}

	private void init(int numThreads, int minDispatch, int maxDispatch, int exactDispatch, boolean rootDispatchOnly) {
		init(numThreads, minDispatch, maxDispatch, exactDispatch, rootDispatchOnly, -1, -1, null);
	}

	private void init(int numThreads, int minDispatch, int maxDispatch, int exactDispatch, boolean rootDispatchOnly,
			int startIndex, int endIndex, LocalDateTime endTime) {
		if (SINGLE_NODE_NO_MPJ) {
			this.rank = 0;
			this.size = 1;
			rootDispatchOnly = true;
		} else {
			this.rank = MPI.COMM_WORLD.Rank();
			this.size = MPI.COMM_WORLD.Size();
		}

		try {
			hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {}
		
		this.numThreads = numThreads;
		this.minDispatch = minDispatch;
		this.maxDispatch = maxDispatch;
		this.exactDispatch = exactDispatch;
		this.rootDispatchOnly = rootDispatchOnly;

		this.startIndex = startIndex;
		this.endIndex = endIndex;
		
		if (endTime != null) {
			// if job end time is known, schedule a job to abort before wall time is reached
			// this helps to ensure a clean exit. killed jobs that didn't shut down properly
			// can sometimes inhibit future runs on the same compute node
			try {
				LocalDateTime now = LocalDateTime.now();
				Duration duration = Duration.between(now, endTime);
				long secs = duration.get(ChronoUnit.SECONDS);
				if (rank == 0)
					debug("End time in "+secs+" s = "+smartTimePrint(secs*1000l));
				long buffer;
				if (secs > 10*60*60)
					// 120s buffer for 10+ hour jobs
					buffer = 120;
				else if (secs > 60*60)
					// 60s buffer for 1+ hour jobs
					buffer = 60;
				else if (secs > 600)
					// 30s buffer for 10m+ jobs
					buffer = 30;
				else
					buffer = 0;
				if (buffer > 0) {
					// only bother for long running jobs
					long terminateSecs = secs - buffer;
					if (rank == 0)
						debug("Terminating in "+terminateSecs+" s = "+smartTimePrint(terminateSecs*1000l));
					timeoutScheduler = Executors.newScheduledThreadPool(1);
					timeoutScheduler.schedule(new TimeoutAbortRunnable(), terminateSecs, TimeUnit.SECONDS);
//					System.out.println("Scheduled timeout");
				}
			} catch (Exception e) {
				System.err.println("Exception creating wall clock abort thread");
				e.printStackTrace();
			}
		}
	}

	protected int getNumThreads() {
		return numThreads;
	}

	protected void debug(String message) {
		debug(rank, hostname, message);
	}

	protected String getDebugText(String message) {
		return getDebugText(rank, hostname, message);
	}

	protected static String getDebugText(int rank, String hostname, String message) {
		if (hostname == null)
			return "["+df.format(new Date())+" Process "+rank+"]: "+message;
		else
			return "["+df.format(new Date())+" ("+hostname+") Process "+rank+"]: "+message;
	}

	protected static void debug(int rank, String hostname, String message) {
		if (!D)
			return;
		System.out.flush();
		System.out.println(getDebugText(rank, hostname, message));
		System.out.flush();
	}

	/**
	 * @return the total number of tasks to be executed
	 */
	protected abstract int getNumTasks();
	
	/**
	 * @return a collection (HashSet suggested for efficiency) of indexes which are already completed, e.g. if restarting,
	 * and can be skipped by the dispatcher, or null (default implementation) if everything should be calculated
	 */
	protected Collection<Integer> getDoneIndexes() {
		return null;
	}

	public void run() throws IOException, InterruptedException {
		if (rank == 0) {
			// launch the dispatcher
			if (startIndex < 0)
				startIndex = 0;
			if (endIndex < 0)
				endIndex = getNumTasks();
			dispatcher = new DispatcherThread(size, getNumTasks(),
					minDispatch, maxDispatch, exactDispatch, shuffle, startIndex, endIndex, postBatchHook, getDoneIndexes());
			if (rootDispatchOnly) {
				debug("starting dispatcher serially");
				dispatcher.run();
			} else {
				debug("starting dispatcher threaded");
				dispatcher.start();
			}
		}

		int[] my_id = { rank };

		int[] batch_lengh_buf = new int[1];

		while (true) {
			if (rank == 0 && rootDispatchOnly)
				break;

			int[] batch;
			if (dispatcher == null) {
				// this is a non-root thread, use MPJ to get the next batch

				debug("sending READY message");
				// report to dispatcher as ready
				MPI.COMM_WORLD.Send(my_id, 0, 1, MPI.INT, 0, TAG_READY_FOR_BATCH);

				debug("receiving batch lengh");
				// receive a new batch length
				MPI.COMM_WORLD.Recv(batch_lengh_buf, 0, 1, MPI.INT, 0, TAG_NEW_BATCH_LENGH);

				if (batch_lengh_buf[0] == 0) {
					debug("DONE!");
					// we're done
					break;
				}

				batch = new int[batch_lengh_buf[0]];

				debug("receiving batch of length "+batch.length);
				MPI.COMM_WORLD.Recv(batch, 0, batch.length, MPI.INT, 0, TAG_NEW_BATCH);
			} else {
				debug("getting next batch directly");
				batch = dispatcher.getNextBatch(rank);

				if (batch == null || batch.length == 0) {
					debug("DONE!");
					// we're done
					break;
				} else {
					debug("receiving batch of length "+batch.length);
				}
			}

			// now calculate the batch
			debug("calculating batch");
			try {
				calculateBatch(batch);
			} catch (Exception e) {
				abortAndExit(e);
			}
		}

		debug("waiting for other processes with Barrier()");

		// wait for everyone
		if (!SINGLE_NODE_NO_MPJ)
			MPI.COMM_WORLD.Barrier();
		try {
			doFinalAssembly();
		} catch (Exception e) {
			abortAndExit(e);
		}

		debug("Process "+rank+" DONE!");
		
		if (timeoutScheduler != null) {
			try {
				timeoutScheduler.shutdownNow();
			} catch (Exception e) {
				debug("Exception during timout cancellation");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Called when a set of tasks are to be executed by this worker. The batch array contains task indexes (0-based)
	 * of each task to be executed. Threading is to be implemented here if applicable, using getNumThreads().
	 * 
	 * @param batch array of task indexes (0-based) which should be executed
	 * @throws Exception
	 */
	protected abstract void calculateBatch(int[] batch) throws Exception;

	/**
	 * Called when all tasks have been executed across all workers. This is where any post processing of results should
	 * take place, for example, gathering results to the node with rank 0 before writing to a file. MPI commands can be used here if needed.
	 * 
	 * @throws Exception
	 */
	protected abstract void doFinalAssembly() throws Exception;

	protected static Options createOptions() {
		Options ops = new Options();

		Option threadsOption = new Option("t", "threads", true,
				"Number of calculation threads on each node. Default is the number" +
						" of available processors (in this case: "+Runtime.getRuntime().availableProcessors()+")");
		threadsOption.setRequired(false);
		ops.addOption(threadsOption);

		Option minDispatchOption = new Option("min", "min-dispatch", true, "Minimum number of tasks to dispatch" +
				" to a compute node at a time. Default: "+MIN_DISPATCH_DEFAULT);
		minDispatchOption.setRequired(false);
		ops.addOption(minDispatchOption);

		Option maxDispatchOption = new Option("max", "max-dispatch", true, "Maximum number of tasks to dispatch" +
				" to a compute node at a time. Actual tasks per node will never be greater than the number of" +
				" sites divided by the number of nodes. Default: "+MAX_DISPATCH_DEFAULT);
		maxDispatchOption.setRequired(false);
		ops.addOption(maxDispatchOption);

		Option exactDispatchOption = new Option("exact", "exact-dispatch", true, "Exact number of tasks to dispatch" +
				" to a compute node at a time. Default is calculated from min/max and number of tasks left.");
		exactDispatchOption.setRequired(false);
		ops.addOption(exactDispatchOption);

		Option rootDispatchOnlyOption = new Option("rdo", "root-dispatch-only", false, "Flag for root node only" +
				"dispatching tasks and not calculating itself");
		rootDispatchOnlyOption.setRequired(false);
		ops.addOption(rootDispatchOnlyOption);

		Option deadlockOption = new Option("dead", "deadlock", false,
				"If supplied, dedlock detection will be enabled (no recovery, however).");
		deadlockOption.setRequired(false);
		ops.addOption(deadlockOption);

		Option startIndexOption = new Option("start", "start-index", true, "If supplied, will calculate tasks starting at the"
				+ " given index, includsive. Default is zero.");
		startIndexOption.setRequired(false);
		ops.addOption(startIndexOption);

		Option endIndexOption = new Option("end", "end-index", true, "If supplied, will calculate tasks up until the"
				+ " given index, exclusive. Default is the number of tasks.");
		endIndexOption.setRequired(false);
		ops.addOption(endIndexOption);

		Option endTimeOption = new Option("endtime", "end-time", true, "If supplied and end time is more than 10m from job start,"
				+ " job will be aborted 15-60s before this time to avoid being killed externally. ISO 8601 local datetime, "
				+ "e.g. '2018-02-23T15:22:59'");
		endTimeOption.setRequired(false);
		ops.addOption(endTimeOption);

		return ops;
	}
	
	public static ArgumentBuilder argumentBuilder() {
		return new ArgumentBuilder();
	}
	
	public static class ArgumentBuilder {
		List<String> args;
		
		private ArgumentBuilder() {
			args = new ArrayList<String>();
		}
		
		public ArgumentBuilder minDispatch(int minDispatch) {
			args.add("--min-dispatch "+minDispatch);
			return this;
		}
		
		public ArgumentBuilder maxDispatch(int maxDispatch) {
			args.add("--max-dispatch "+maxDispatch);
			return this;
		}
		
		public ArgumentBuilder exactDispatch(int exactDispatch) {
			args.add("--exact-dispatch "+exactDispatch);
			return this;
		}
		
		public ArgumentBuilder threads(int threads) {
			args.add("--threads "+threads);
			return this;
		}
		
		public ArgumentBuilder rootDispatchOnly() {
			args.add("--root-dispatch-only");
			return this;
		}
		
		public ArgumentBuilder deadlockDetection() {
			args.add("--deadlock");
			return this;
		}
		
		public ArgumentBuilder startIndex(int startIndex) {
			args.add("--start-index "+startIndex);
			return this;
		}
		
		public ArgumentBuilder endIndex(int endIndex) {
			args.add("--end-index "+endIndex);
			return this;
		}
		
		public ArgumentBuilder endTime(String endTime) {
			args.add("--end-time "+endTime);
			return this;
		}
		
		public ArgumentBuilder endTimeSlurm() {
			args.add("--end-time `scontrol show job $SLURM_JOB_ID | egrep --only-matching 'EndTime=[^ ]+' | cut -c 9-`");
			return this;
		}
		
		public List<String> getArgs() {
			return args;
		}
		
		public String build() {
			return build(" ");
		}
		
		public String build(String separator) {
			return Joiner.on(separator).join(args);
		}
	}

	protected static String[] initMPJ(String[] args) {
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				abortAndExit(e);
			}
		});
		if (SINGLE_NODE_NO_MPJ)
			return args;
		try {
			return MPI.Init(args);
		} catch (Throwable t) {
			abortAndExit(t);
			return null; // not accessible
		}
	}

	protected static CommandLine parse(Options options, String args[], Class<?> clazz) {
		try {
			CommandLineParser parser = new DefaultParser();

			CommandLine cmd = parser.parse(options, args);
			return cmd;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(
					getClassNameWithoutPackage(clazz),
					options, true );
			abortAndExit(2);
			return null; // not accessible
		}
	}

	protected static void finalizeMPJ() {
		if (deadlock != null)
			deadlock.kill();
		if (!SINGLE_NODE_NO_MPJ)
			MPI.Finalize();
		System.exit(0);
	}

	public static void abortAndExit(int ret) {
		abortAndExit(null, ret);
	}

	public static void abortAndExit(Throwable t) {
		abortAndExit(t, 1);
	}

	public static void abortAndExit(Throwable t, int ret) {
		try {
			if (t != null)
				t.printStackTrace();
			if (deadlock != null)
				deadlock.kill();
			if (!SINGLE_NODE_NO_MPJ)
				MPI.COMM_WORLD.Abort(ret);
		} catch (Throwable t1) {
			System.err.println("Excpetion during abort");
			t1.printStackTrace();
		} finally {
			System.out.println("Exiting");
			System.exit(ret);
		}
	}
	
	private static class TimeoutAbortRunnable implements Runnable {

		@Override
		public void run() {
			System.out.println("EXCEEDED TIMEOUT, ABORTING");
			abortAndExit(2);
		}
		
	}

}
