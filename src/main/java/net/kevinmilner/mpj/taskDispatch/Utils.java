package net.kevinmilner.mpj.taskDispatch;

import java.io.File;
import java.text.Collator;
import java.util.Comparator;

class Utils {
	
	/**
	 * Returns the given exception as a runtime exception. If it already is a runtime
	 * exception then it will be simply cast and returned, otherwise a new runtime exception
	 * will be created with this as it's cause.
	 * 
	 * @param t
	 */
	static RuntimeException asRuntimeException(Throwable t) {
		if (t instanceof RuntimeException)
			return (RuntimeException)t;
		return new RuntimeException(t);
	}
	
	/**
	 * Returns class name without package declaration
	 * 
	 * @param theClass
	 * @return
	 */
	static String getClassNameWithoutPackage(Class<?> theClass) {
		String name = theClass.getName();
		String[] split = name.split("\\.");
		return split[split.length-1];
	}
	
	/**
	 * File name comparator utility
	 * @author kevin
	 *
	 */
	static class FileNameComparator implements Comparator<File> {
		
		// A Collator for String comparisons
		private Collator c = Collator.getInstance();
		
		@Override
		public int compare(File f1, File f2) {
			if (f1 == f2) return 0;
			
			// promote directories for file-directory pairs
			if (f1.isDirectory() && f2.isFile()) return -1;
			if (f1.isFile() && f2.isDirectory()) return 1;
			
			// use Collator for file-file and dir-dir pairs
			return c.compare(f1.getName(), f2.getName());
		}
	}
	
	/**
	 * Class for tracking the minimum and maximum values of a set of data.
	 */
	static class MinMaxAveTracker {
		private double min = Double.POSITIVE_INFINITY;
		private double max = Double.NEGATIVE_INFINITY;
		private double tot = 0;
		private int num = 0;

		/**
		 * Add a new value to the tracker. Min/Max/Average will be updated.
		 * 
		 * @param val value to be added
		 */
		public void addValue(double val) {
			if (val < min) min = val;
			if (val > max) max = val;
			tot += val;
			num++;
		}

		/**
		 * Returns the minimum value that has been added to this tracker, or positive infinity if
		 * no values have been added.
		 * 
		 * @return minimum value
		 */
		public double getMin() {
			return min;
		}

		/**
		 * Returns the maximum value that has been added to this tracker, or negative infinity if
		 * no values have been added.
		 * 
		 * @return maximum value
		 */
		public double getMax() {
			return max;
		}

		/**
		 * Computes the average of all values that have been added to this tracker.
		 * 
		 * @return the average of all values that have been added to this tracker.
		 */
		public double getAverage() {
			return tot / (double) num;
		}

		/**
		 * 
		 * @return total number of values added to this tracker.
		 */
		public int getNum() {
			return num;
		}

		@Override
		public String toString() {
			return "min: " + min + ", max: " + max + ", avg: " + getAverage() + ", tot: "+tot;
		}
	}

}
