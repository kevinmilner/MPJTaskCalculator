package edu.usc.kmilner.mpj.taskDispatch;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.junit.Test;

public class DispatcherThreadTest {
	
	private static final Random r = new Random();
	
	private static int randomSize(int min, int max) {
		return min + r.nextInt(max - min);
	}

	@Test
	public void testAllTasksExecuted() {
		int numTasks = randomSize(50, 500);
		DispatcherThread dispatcher = new DispatcherThread(10, numTasks, 5, numTasks, true);
		
		HashSet<Integer> processedIndexes = new HashSet<>();
		
		int numProcessed = 0;
		while (true) {
			int[] batch = dispatcher.getNextBatch(0);
			
			// make sure no duplicates
			for (int index : batch)
				assertTrue("Duplicate index detected!", processedIndexes.add(index));
			
			numProcessed += batch.length;
			
			if (batch.length == 0)
				break;
		}
		assertEquals("Not all tasks dispatched!", numTasks, numProcessed);
	}
	
	@Test
	public void testExecutionOrder() {
		int numTasks = randomSize(50, 500);
		DispatcherThread dispatcher = new DispatcherThread(10, numTasks, 5, numTasks, false);
		
		int expectedIndex = 0;
		
		while (expectedIndex < numTasks) {
			int[] batch = dispatcher.getNextBatch(0);
			
			for (int index : batch)
				assertEquals("Events dispatched out of order with shuffle=false", expectedIndex++, index);
		}
	}
	
	@Test
	public void testPostBatchHook() {
		int numTasks = randomSize(50, 500);
		int procs = randomSize(2, 10);
		
		DummyPostBatchHook hook = new DummyPostBatchHook();
		DispatcherThread dispatcher = new DispatcherThread(procs, numTasks, procs, numTasks, -1, false, 0, numTasks, hook, null);
		
		boolean[] dones = new boolean[procs];
		
		HashMap<Integer, int[]> prevBatches = new HashMap<>();
		
		while (!allDone(dones)) {
			int index = randomNotDoneIndex(dones);
			
			hook.setNextExpected(prevBatches.remove(index), index);
			
			int[] batch = dispatcher.getNextBatch(index);
			
			// make sure it was processed
			assertNull("post batch hook not processed as expected", hook.expectedBatch);
			
			if (batch.length == 0)
				dones[index] = true;
			else
				prevBatches.put(index, batch);
		}
	}
	
	private static boolean allDone(boolean[] dones) {
		for (boolean done : dones)
			if (!done)
				return false;
		return true;
	}
	
	private static int randomNotDoneIndex(boolean[] dones) {
		ArrayList<Integer> notDones = new ArrayList<>();
		for (int i=0; i<dones.length; i++)
			if (!dones[i])
				notDones.add(i);
		assertFalse(notDones.isEmpty());
		return notDones.get(r.nextInt(notDones.size()));
	}
	
	private class DummyPostBatchHook implements PostBatchHook {
		
		private int[] expectedBatch;
		private int expectedIndex;

		public void setNextExpected(int[] expectedBatch, int expectedIndex) {
			this.expectedBatch = expectedBatch;
			this.expectedIndex = expectedIndex;
		}

		@Override
		public void batchProcessed(int[] batch, int processIndex) {
			assertNotNull("batchProcessed called when not expected", expectedBatch);
			assertEquals("batchProcessed called with wrong process index", expectedIndex, processIndex);
			assertArrayEquals("batchProcessed called with wrong batch", expectedBatch, batch);
			
			// clear expected batch so that we can make sure it was executed
			expectedBatch = null;
			expectedIndex = -1;
		}
	}
	
	@Test
	public void testBatchSizes() {
		for (int i=0; i<10; i++) {
			int numTasks = randomSize(50, 500);
			int minSize = randomSize(1, 10);
			int maxSize = randomSize(minSize, numTasks);
			DispatcherThread dispatcher = new DispatcherThread(10, numTasks, minSize, maxSize, true);
			doTestBatchSizes(dispatcher, minSize, maxSize, numTasks);
		}
		
		// test exact dispatch
		for (int i=0; i<10; i++) {
			int numTasks = randomSize(50, 500);
			int exactDispatch = randomSize(1, numTasks);
			DispatcherThread dispatcher = new DispatcherThread(
					10, numTasks, exactDispatch, exactDispatch, exactDispatch, true, 0, numTasks, null, null);
			doTestBatchSizes(dispatcher, exactDispatch, exactDispatch, numTasks);
		}
	}
	
	private void doTestBatchSizes(DispatcherThread dispatcher, int minSize, int maxSize, int numTasks) {
		int numProcessed = 0;
		while (true) {
			int numLeft = numTasks - numProcessed;
			
			int[] batch = dispatcher.getNextBatch(0);
			
			assertTrue("Batch is too large!", batch.length <= maxSize);
			
			if (numLeft >= minSize)
				assertTrue("Batch is too small!", batch.length >= minSize);
			
			numProcessed += batch.length;
			
			if (batch.length == 0)
				break;
		}
	}
	
	@Test
	public void testSubsetIndexes() {
		// endIndex is exclusive
		
		// test start beginning, stop before end
		for (int i=0; i<10; i++) {
			int numTasks = randomSize(50, 500);
			int startIndex = 0;
			int endIndex = randomSize(numTasks/2, numTasks-1);
			doTestSubsetIndexes(numTasks, startIndex, endIndex);
		}
		
		// test start after beginning, stop at end
		for (int i=0; i<10; i++) {
			int numTasks = randomSize(50, 500);
			int startIndex = randomSize(1, numTasks/2);
			int endIndex = numTasks;
			doTestSubsetIndexes(numTasks, startIndex, endIndex);
		}
		
		// test random set in the middle
		for (int i=0; i<10; i++) {
			int numTasks = randomSize(50, 500);
			int startIndex = randomSize(1, numTasks/2);
			int endIndex = randomSize(startIndex+1, numTasks-1);
			doTestSubsetIndexes(numTasks, startIndex, endIndex);
		}
	}
	
	private void doTestSubsetIndexes(int numTasks, int startIndex, int endIndex) {
		DispatcherThread dispatcher = new DispatcherThread(10, numTasks, 1, numTasks, -1, true, startIndex, endIndex, null, null);
		int numProcessed = 0;
		while (true) {
			int[] batch = dispatcher.getNextBatch(0);
			
			// check range
			for (int index : batch) {
				assertTrue("Index below expected range", index >= startIndex);
				assertTrue("Index above expected range", index < endIndex);
			}
			
			numProcessed += batch.length;
			
			if (batch.length == 0)
				break;
		}
		assertEquals("Not all tasks dispatched!", endIndex - startIndex, numProcessed);
	}

}
