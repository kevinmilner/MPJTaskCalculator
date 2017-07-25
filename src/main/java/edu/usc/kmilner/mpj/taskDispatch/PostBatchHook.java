package edu.usc.kmilner.mpj.taskDispatch;

/**
 * Interface for a hook to be run on the worker with rank=0 after the completion of each batch.
 * Can be used to do post batch assembly. Blocks the {@link DispatcherThread} until the batch is
 * processed, for asynchronous implementation, see {@link AsyncPostBatchHook}. Hook is executed the
 * next time a given process requests a batch.
 * @author Kevin Milner
 *
 */
public interface PostBatchHook {
	
	public void batchProcessed(int[] batch, int processIndex);

}
