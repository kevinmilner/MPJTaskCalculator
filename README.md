# MPJTaskCalculator
Framework for executing predefined sets of independent tasks with MPJ (without having to write any MPI commands)

## Build Status
[![Build Status](https://travis-ci.org/kevinmilner/MPJTaskCalculator.svg?branch=master)](https://travis-ci.org/kevinmilner/MPJTaskCalculator)

## Applicable problems
MPJTaskCalculator can be used to execute calculations where the following requirements are met:
* The calculation is split into a number of predefined tasks
* Each task can be executed independently of all other tasks (no dependencies)
* The number of tasks is known at the beginning of the calculation
* Each task has a unique index which is known to all workers
* The number of tasks should be greater than or equal to the number of workers
* You have an MPJ implementation working in your execution environment (e.g. [MPJ Express](http://mpj-express.org/) or [FastMPJ](http://fastmpj.com))

## How it works
MPJTaskCalculator uses an MPJ implementation for intra-process communication (either within a single machine, or in a grid computing environment). The root process (rank=0) starts a dispatcher which manages the list of all tasks to be dispatched. It then dispatches batches of tasks to each worker to be executed. Batches are simply an array of task indexes which are to be executed, the size of each batch is determined from the number of workers and remaining tasks (smaller batches later in the calculation). Whenever a worker completes a batch, it will ask for a new batch until all tasks have been completed. Once all tasks have been dispatched and executed, it will execute any calcultaion-specific post processing, then exit.

## Using MPJTaskCalculator in your project

First you will need to download or build a jar file for MPJTaskCalculator, and set up your MPJ environment. It depends on [Google Guava](https://github.com/google/guava) (developed with release 21) and [Apache Commons CLI 1.4](https://commons.apache.org/proper/commons-cli/). A "fat" jar including these dependencies is provided for convenience.

### Download latest release
Download a release [here](https://github.com/kevinmilner/MPJTaskCalculator/releases). The `mpj-task-calculator-all-<version>.jar` "fat" jar files contain all dependencies (except your MPJ implementation), and the `mpj-task-calculator-<version>.jar` only contain compiled MPJTaskCalculator code.

### Build source code
MPJTaskCalculator uses [Gradle](https://gradle.org/) to build the code. First clone or download master or a branch, then build with these commands:

For a jar file without any dependencies:

`./gradlew jar` on Mac/Linux, or `gradlew.bat jar` on Windows

For a "fat" jar file including dependencies:

`./gradlew fatJar` on Mac/Linux, or `gradlew.bat fatJar` on Windows

This will build a jar file in `builds/lib`

### Obtain and configure an MPJ implementation
MPJTaskCalculator has been tested and works with [MPJ Express](http://mpj-express.org/) and [FastMPJ](http://fastmpj.com). Refer to their documentation to set up MPJ.

## The [MPJTaskCalculator](src/main/java/edu/usc/kmilner/mpj/taskDispatch/MPJTaskCalculator.java) abstract class

Once you have added MPJTaskCalculator to your project, write a new class which extends [MPJTaskCalculator](src/main/java/edu/usc/kmilner/mpj/taskDispatch/MPJTaskCalculator.java)

### Required abstract methods
MPJTaskCalculator is an abstract class which requires implementation of 3 methods:

Abstract Method | Description
------------ | -------------
`protected int getNumTasks()` | Returns the total number of tasks to be executed
`protected void calculateBatch(int[] batch)` | Called when a set of tasks are to be executed by this worker. The batch array contains task indexes (0-based) of each task to be executed. Threading is to be implemented here if applicable, using getNumThreads().
`protected void doFinalAssembly()` | Called when all tasks have been executed across all workers. This is where any post processing of results should take place, for example, gathering results to the node with rank 0 before writing to a file. MPI commands can be used here if needed.

### Dispatching parameters
You will also need to invoke a constructor to set to the dispatching parameters. This can be done either directly, or with an Apache Commons CLI CommandLine instance. Parameters include:

Variable Name | Command Line Argument | Description
-- | -- | --
`numThreads` | `-t/--threads` | Number of calculation threads on each node. Default is the number of available processors
`minDispatch` | `-min/--min-dispatch` | Minimum number of tasks to dispatch to a compute node at a time. Default: 5
`maxDispatch` | `-max/--max-dispatch` | Maximum number of tasks to dispatch to a compute node at a time. Actual tasks per node will never be greater than the number of sites divided by the number of nodes. Default: 100
`exactDispatch` | `-exact/--exact-dispatch` | Exact number of tasks to dispatch to a compute node at a time. Default is calculated from min/max and number of tasks left.
`rootDispatchOnly` | `-rdo/--root-dispatch-only` | Flag for root node only dispatching tasks and not calculating itself
`startIndex` | `start-index` | If supplied, will calculate tasks starting at the given index, includsive. Default is zero.
`endIndex` | `end-index` | If supplied, will calculate tasks up until the given index, exclusive. Default is the number of tasks.

### Simple adding example
A simple example implementation is provided in [MPJ_AddTest](src/main/java/edu/usc/kmilner/mpj/taskDispatch/examples/MPJ_AddTest.java). This example does not incorporate any threading within a worker, that is demonstrated in [MPJ_ThreadedAddTest](src/main/java/edu/usc/kmilner/mpj/taskDispatch/examples/MPJ_ThreadedAddTest.java).

To run this example on a single machine with MPJ-Express on Mac OSX or Linux, assuming that the code has been built with embedded dependencies in a jar located in build/libs/mpj-task-calculator-all-1.0.jar and that the MPJ_HOME environmental variable has been set:

`java -jar ${MPJ_HOME}/lib/starter.jar -classpath build/libs/mpj-task-calculator-all-1.0.jar edu.usc.kmilner.mpj.taskDispatch.examples.MPJ_AddTest`

Refer to the documentation with your MPJ implementation for instructions for running in more complex environments.

## Monitoring running jobs/parsing logs

The [MPJTaskLogStatsGen](src/main/java/edu/usc/kmilner/mpj/taskDispatch/MPJTaskLogStatsGen.java) class is capable of reading the STDOUT from a running or completed MPJTaskCalculator calculation. It can be used to estimate remaining runtime, among other things. Simply call the class with the pass to the STDOUT of your job as an argument. Times are based on the last timestamp encountered in the log file, or the current system time (in parentheses). For example:

```
kevin@steel:~/workspace/MPJTaskCalculator$ java -cp build/libs/mpj-task-calculator-all-1.0.jar edu.usc.kmilner.mpj.taskDispatch.MPJTaskLogStatsGen /tmp/test_job_stdout.txt 
Done parsing log

Process 0 (hpc4209):	lastContact: 13.66 m	(2.78 h)	batches: 09/10	tasks: 09000/10000	avg: 871.28 ms	RUNNING: 1000
Process 1 (hpc4210):	lastContact: 14.10 m	(2.78 h)	batches: 09/10	tasks: 09000/10000	avg: 867.21 ms	RUNNING: 1000
Process 2 (hpc4211):	lastContact: 10.87 m	(2.73 h)	batches: 09/10	tasks: 09000/09788	avg: 888.73 ms	RUNNING: 788
Process 3 (hpc4212):	lastContact: 14.34 m	(2.79 h)	batches: 09/10	tasks: 09000/10000	avg: 865.68 ms	RUNNING: 1000
Process 4 (hpc4213):	lastContact: 12.07 m	(2.75 h)	batches: 09/10	tasks: 09000/09884	avg: 880.61 ms	RUNNING: 884
Process 5 (hpc4214):	lastContact: 6.00 s	(2.55 h)	batches: 10/11	tasks: 10000/10560	avg: 864.35 ms	RUNNING: 560
Process 6 (hpc4215):	lastContact: 12.87 m	(2.76 h)	batches: 09/10	tasks: 09000/09936	avg: 875.49 ms	RUNNING: 936
Process 7 (hpc4216):	lastContact: 1.33 m	(2.57 h)	batches: 10/11	tasks: 10000/10627	avg: 856.90 ms	RUNNING: 627
Process 8 (hpc4217):	lastContact: 0.00 ms	(2.55 h)	batches: 10/11	tasks: 10000/10528	avg: 864.97 ms	RUNNING: 528
Process 9 (hpc4218):	lastContact: 13.14 m	(2.77 h)	batches: 09/10	tasks: 09000/09991	avg: 874.04 ms	RUNNING: 991
Process 10 (hpc4219):	lastContact: 2.10 m	(2.58 h)	batches: 10/11	tasks: 10000/10703	avg: 852.34 ms	RUNNING: 703
Process 11 (hpc4220):	lastContact: 42.03 s	(2.56 h)	batches: 10/11	tasks: 10000/10592	avg: 860.75 ms	RUNNING: 592
Process 12 (hpc4221):	lastContact: 1.93 m	(2.58 h)	batches: 10/11	tasks: 10000/10664	avg: 853.27 ms	RUNNING: 664
Process 13 (hpc4222):	lastContact: 10.50 m	(2.72 h)	batches: 09/10	tasks: 09000/09745	avg: 891.08 ms	RUNNING: 745
Process 14 (hpc4223):	lastContact: 14.07 m	(2.78 h)	batches: 09/10	tasks: 09000/10000	avg: 867.21 ms	RUNNING: 1000
Process 15 (hpc4224):	lastContact: 11.00 m	(2.73 h)	batches: 09/10	tasks: 09000/09835	avg: 887.61 ms	RUNNING: 835
Process 16 (hpc4225):	lastContact: 13.67 m	(2.78 h)	batches: 09/10	tasks: 09000/10000	avg: 869.99 ms	RUNNING: 1000
Process 17 (hpc4226):	lastContact: 13.74 m	(2.78 h)	batches: 09/10	tasks: 09000/10000	avg: 869.60 ms	RUNNING: 1000

Longest current time without contact: Process 3 (hpc4212): 14.34 m (2.79 h)
Most recent contact from current date: Process 8 (hpc4217): 2.55 h

182853/191828 (95.32 %) dispatched (8975 left)
168000/191828 (87.58 %) completed (23828 left)
14853 in process on 18/18 nodes, batch sizes [528 1000]
Calc durations (note: threading effects ignored):
	Range: [834.59 ms 924.79 ms]
	Average: 869.66 ms
	Total: 2.44 m
Batch durations:
	Range: [13.91 m 15.41 m]
	Average: 14.49 m
	Total: 1.69 d

DONE? false

Current duration: 2.41 h (4.96 h)
Total rate: 19.37 tasks/s

Estimating time left, assuming average task runtime & ideal dispatching.
Estimating time left from last known date (14:22:04.539):
	Time left: 10.30 m
Estimating time left from current date (16:54:59.518):
	Time left: 7.24 m
Estimated total duration: 2.58 h
kevin@steel:~/workspace/MPJTaskCalculator$
```