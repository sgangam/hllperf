
package personal;

import java.io.IOException;
import java.util.Random;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import net.agkn.hll.*;

import com.google.common.hash.Hasher;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


/**
 * Microbenchmarks based on Hyperloglog from stream-lib and java-hll. We measure the
 * performance in terms of merge time and insertion time.
 */
public class Microbenchmarks {

	private static final int EXPERIMENT_COUNT = 10; // run experiments for 10 times
	// HLL/HLLPlus parameters
	private static final int PRECISION = 10;
	private static final int SPARSE_PRECISION = 18;
	private static final int REGISTER_WIDTH = 5;

	private static final double SPLIT_FRAC_START = 0.1;
	private static final double SPLIT_FRAC_END = 0.5;
	private static final double SPLIT_FRAC_INC = 0.1;
	private static final int MURMUR_HAS_BITS = 128;
	
	final HashFunction hash = Hashing.goodFastHash(MURMUR_HAS_BITS);

    public static void main(String[] args) throws Exception {

    	Microbenchmarks mb = new Microbenchmarks();

    	mb.memoryStats();
    	mb.memoryTests(2000, 100);
    	
    	long cardinality = 10000000L;
    	long startItem = 10L;
    	int speedupFactor = 5; // must be > 1 to avoid infinite loop
    	mb.insertionTests(cardinality, startItem, speedupFactor);
		
		long totalCard = 100000;
		long commonCard = 10000;
		mb.mergeTests(totalCard, commonCard);
 	}
   
    private void memoryStats() throws IOException, CardinalityMergeException {

		System.out.println("Memory statistics"); 
		System.out.println("---------------------------------------------------"); 

    	int cardinality = 1200;
    	int interval = 50;
		ICardinality s1 = new HyperLogLogPlus(10, 0);
		ICardinality s2 = new HyperLogLogPlus(10, 10);
		ICardinality s3 = new HyperLogLogPlus(10, 18);
		ICardinality sm = new HyperLogLogPlus(10, 10);
		
		HLL akHll = new HLL(PRECISION, REGISTER_WIDTH, -1, true, HLLType.EMPTY);
		
		Random prng = new Random(1);
		for (int i = 0; i < cardinality; i++) {
			long item = prng.nextLong();
			s1.offer(item);
			s2.offer(item);
			s3.offer(item);

			Hasher hasher = hash.newHasher();
			hasher.putLong(item);
			akHll.addRaw(hasher.hash().asLong());
			
			ICardinality temp = new HyperLogLogPlus(10, 10);
			temp.offer(item);
			sm = sm.merge(temp);
			
			if (i % interval == 0) {
				System.out.println("Cardinality:" + i + " || Bytes used:" +
						" s1:" + s1.getBytes().length +
						", s2:" + s2.getBytes().length +
						", s3:" + s3.getBytes().length +
						", skmerge:" + sm.getBytes().length +
						", akHLL:" + akHll.toBytes().length);
			}
		}
		System.out.println();
	}

	/** 
	 * Compares if sizeof() and serialized version give the same memory footprint.
	 * Unfortunately they are not the same possibly due to a bug in stream-lib
	 * 
	 * @param cardinality
	 * @throws CardinalityMergeException 
	 */
    private void memoryTests(int cardinality, int interval) 
    		throws IOException, CardinalityMergeException {

		ICardinality hll = new HyperLogLog(PRECISION);
		ICardinality hllPlus = new HyperLogLogPlus(PRECISION, SPARSE_PRECISION);
		
		System.out.println("\n Comparison of sizeof() and getBytes.length() from stream-lib hll"); 
		System.out.println("---------------------------------------------------"); 

		Random prng = new Random(1);
		for (int i = 0; i < cardinality; i++) {
			long item = prng.nextLong();

			hll.offer(item);
			hllPlus.offer(item);
			if (i % interval == 1) {
				System.out.println("Cardinality:" + i + " hllBytes:" + hll.getBytes().length +
						" hll-sizeof(): " + hll.sizeof() + " hllPlus-sizeof():" +
						hllPlus.getBytes().length + " hllPlusBytes:" + hllPlus.sizeof());
			}
		}
	}

	/** 
	 * Estimates the time taken for inserting an item into an HLL sketch on
	 * average. This is a part of the investigation on whether HLL sketches are a
	 * good fit to estimate unique measures.
	 */
    private void insertionTests(long cardinality, long startItem, int speedupFactor) {

		System.out.println("\nSketch Insertion Stats:");
		System.out.println("---------------------------------------------------"); 
		Random prng = new Random(1);
		for (long card = startItem; card <= cardinality; card *= speedupFactor) {
			double avgHLLCard = 0; double avgTimeHLL = 0; double avgHLLError = 0;
			double avgHLLPlusCard = 0; double avgTimeHLLPlus = 0; double avgHLLPlusError = 0;
			double avgAkHLLCard = 0; double avgTimeAkHLL = 0; double avgAkHLLError = 0;

			for (int sample = 1; sample <= EXPERIMENT_COUNT; sample++) {
				InsertMergeTimer im = new InsertMergeTimer();
				for (int i = 0; i < card; i++) {
					long item = prng.nextLong();
					im.insert(item);
				}
				long cardHLL = im.getHLLCard(); 
				long cardHLLPlus = im.getHLLPlusCard();
				long cardAkHLL = im.getHLLPlusCard();
				
				double errorHLL = Math.abs((card - cardHLL) * 100.0 / card);
				double errorHLLPlus = Math.abs((card - cardHLLPlus) * 100.0 / card);
				double errorAkHLL = Math.abs((card - cardAkHLL) * 100.0 / card);

				double timeHLL = im.getInsertTimeHLL(); 
				double timeHLLPlus = im.getInsertTimeHLLPlus();
				double timeAkHLL = im.getInsertTimeAkHLL();

				avgHLLError += errorHLL; avgHLLPlusError += errorHLLPlus; avgAkHLLError += errorAkHLL;
				avgHLLCard += cardHLL; avgHLLPlusCard += cardHLLPlus; avgAkHLLCard += cardAkHLL; 
				avgTimeHLL += timeHLL; avgTimeHLLPlus += timeHLLPlus; avgTimeAkHLL += timeAkHLL; 
			}
			avgHLLCard /= EXPERIMENT_COUNT;
			avgTimeHLL /= EXPERIMENT_COUNT;
			avgHLLError /= EXPERIMENT_COUNT;

			avgHLLPlusCard /= EXPERIMENT_COUNT; 
			avgTimeHLLPlus /= EXPERIMENT_COUNT; 
			avgHLLPlusError /= EXPERIMENT_COUNT;

			avgAkHLLCard /= EXPERIMENT_COUNT;
			avgTimeAkHLL /= EXPERIMENT_COUNT;
			avgAkHLLError /= EXPERIMENT_COUNT;

			System.out.println("Cardinality:" + card + " " +
					"Average Error (%):" + avgHLLError + ", " + avgHLLPlusError + ", " + avgAkHLLError +
					" Average Time (ms): " + avgTimeHLL + ", " + avgTimeHLLPlus +  ", " + avgTimeAkHLL+ 
					" Average Card: " + avgHLLCard + ", " + avgHLLPlusCard + ", " + avgAkHLLCard);
		}
	}

    /**
     * Estimates the time taken to merge HLL sketches. This is a part of the
     * investigation on whether HLL sketches are a good fit to estimate unique
     * measures.
     */
	private void mergeTests(long cardinality, long commonCard) throws CardinalityMergeException {
		System.out.println("\nSketch Merging Stats:");
		System.out.println("---------------------------------------------------"); 
		Random prng = new Random(1);
			for (double frac = SPLIT_FRAC_START; frac <= SPLIT_FRAC_END; frac += SPLIT_FRAC_INC) {
				InsertMergeTimer im1 = new InsertMergeTimer();
				InsertMergeTimer im2 = new InsertMergeTimer();
				// Insert a fraction of items in im1 and the remaining in im2
				for (int i = 0; i < cardinality; i++) {
					if (i < cardinality * frac) {
						long item = prng.nextLong();
						im1.insert(item);
					} else {
						long item = prng.nextLong();
						im2.insert(item);
					}
				}
				for (long i = 0; i < commonCard; i++) {
					im1.insert(i);
					im2.insert(i);
				}
				im1.add(im2);

				long mergedCard = cardinality + commonCard;
				double errorHLL = Math.abs((mergedCard - im1.getHLLCard()) * 100.0 / mergedCard);
				double errorHLLPlus = Math.abs((mergedCard - im1.getHLLPlusCard()) * 
						100.0 / mergedCard);
			
				double errorAkHLL = Math.abs((mergedCard - im1.getAkHLLCard()) * 100.0 / mergedCard);

				System.out.println(frac + " " + mergedCard +
						" EstimatedCard:" + im1.getHLLCard() + ", " + im1.getHLLPlusCard() + ", " + im1.getAkHLLCard() + "   " +
						" Times (ms):" + im1.getMergeTimeHLL() + ", " + im1.getMergeTimeHLLPlus() + ", " + im1.getMergeTimeAkHLL() + "   " +
						" Error (%):" + errorHLL + ", " + errorHLLPlus + ", " + errorAkHLL + "  " +
						" Bytes:" + im1.getHLLBytes() + ", " + im1.getHLLPlusBytes() + ", " + im1.getAkHLLBytes());
			}
	}
	
    /**
     * A class the keeps track of the times taken for insertion and merging on
     * stream-lib Hyperloglog library and java-hll
     */
	public class InsertMergeTimer {
		
		private ICardinality hll, hllPlus;
		private HLL akHll;

		private double insertCount, mergeCount;
		private double insertTimeHLL, insertTimeHLLPlus, insertTimeAkHLL;
		private double mergeTimeHLL, mergeTimeHLLPlus, mergeTimeAkHLL;

		InsertMergeTimer() {

			insertCount = 0;
			mergeCount = 0;

			hll = new HyperLogLog(PRECISION);
			hllPlus = new HyperLogLogPlus(PRECISION, SPARSE_PRECISION);
			akHll = new HLL(PRECISION, REGISTER_WIDTH, -1, true, HLLType.EMPTY);
		}

	    /**
	     * Inserts object into both HLL and HLLPlus
	     */
		void insert(Long item) {
			
			String strItem = item.toString(); 
			insertCount++;
			double startTime = System.nanoTime();
			hll.offer(strItem);
			double endTime = System.nanoTime();
			insertTimeHLL += (endTime - startTime);
			//for HLLPlus
			startTime =  System.nanoTime();
			hllPlus.offer(strItem);
			endTime =  System.nanoTime();
			insertTimeHLLPlus += (endTime - startTime);
			// for AkHLL
			startTime =  System.nanoTime();
			Hasher hasher = hash.newHasher();
			hasher.putString(strItem);
			akHll.addRaw(hasher.hash().asLong());
			endTime =  System.nanoTime();
			insertTimeAkHLL += (endTime - startTime);
		}

	    /**
	     * Merges two HLLs and HLLPlues to estimate times
	     */
		void add(InsertMergeTimer im) throws CardinalityMergeException {
			
			mergeCount++;
			double startTime, endTime;
			startTime =  System.nanoTime();
			hll = hll.merge(im.hll);
			endTime =  System.nanoTime();
			mergeTimeHLL += (endTime - startTime);
			//for HLLPlus
			startTime =  System.nanoTime();
			hllPlus = hllPlus.merge(im.hllPlus);
			endTime =  System.nanoTime();
			mergeTimeHLLPlus += (endTime - startTime);
		
			//for akHLL
			startTime =  System.nanoTime();
			akHll.union(im.akHll);
			endTime =  System.nanoTime();
			mergeTimeAkHLL += (endTime - startTime);
		}
		
		private double getInsertTimeHLL() {
			// convert nano to milli seconds
			return (insertTimeHLL / insertCount) / 1000000.0; 
		}

		private double getInsertTimeHLLPlus() {
			return (insertTimeHLLPlus / insertCount) / 1000000.0; 
		}

		private double getInsertTimeAkHLL() {
			return (insertTimeAkHLL / insertCount) / 1000000.0; 
		}
		 
		private double getMergeTimeHLL() {
			return (mergeTimeHLL / mergeCount) / 1000000.0; 
		}

		private double getMergeTimeHLLPlus() {
			return (mergeTimeHLLPlus / mergeCount) / 1000000.0; 
		}

		private double getMergeTimeAkHLL() {
			return (mergeTimeAkHLL / mergeCount) / 1000000.0; 
		}

		private long getHLLCard() {
			return hll.cardinality();
		}

		private long getHLLPlusCard() {
			return hllPlus.cardinality();
		}

		private long getAkHLLCard() {
			return akHll.cardinality();
		}

		private long getHLLBytes() {
			return hll.sizeof();
		}

		private long getHLLPlusBytes() {
			return hllPlus.sizeof();
		}

		private long getAkHLLBytes() {
			return akHll.toBytes().length;
		}
	}
	
}