package de.hhu.bsinfo.dxgraph.run;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorMultiLevel;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorWithStartPos;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifo;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifoNaive;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.HalfConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.TreeSetFifo;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.eval.EvaluationTables;
import de.hhu.bsinfo.utils.eval.Stopwatch;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Benchmark and compare execution time of various frontier lists used for BFS in dxgraph.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class BFSFrontierBenchmarks extends AbstractMain {

	private static final Argument ARG_ITEM_COUNT = new Argument("itemCount", "100", true, "Number of items to add and get from the list. "
			+ "Make sure this is a multiple of the thread count, otherwise the multi threaded part will fail on validation");
	private static final Argument ARG_THREADS = new Argument("threads", "2", true, "Number of threads for the multi threaded section");
	private static final Argument ARG_ITEM_COUNT_RAND_FILL_RATE = new Argument("itemCountRandDistFillRate", "1.0", true, "Enables random "
			+ "distribution if value is < 1.0 and > 0.0 when pushing items and defines the fill rate for the vector with 1.0 being 100%, i.e. full vector"); 
	
	private static final boolean MS_PRINT_READABLE_TIME = false; 
	
	private EvaluationTables m_tables = null;
	
	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		AbstractMain main = new BFSFrontierBenchmarks();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	protected BFSFrontierBenchmarks() {
		super("Test the various BFS frontier implementations and measure execution time.");
		
	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_ITEM_COUNT);
		p_arguments.setArgument(ARG_THREADS);
		p_arguments.setArgument(ARG_ITEM_COUNT_RAND_FILL_RATE);
	}

	@Override
	protected int main(ArgumentList p_arguments) 
	{
		int itemCount = p_arguments.getArgument(ARG_ITEM_COUNT).getValue(Integer.class);
		int threads = p_arguments.getArgument(ARG_THREADS).getValue(Integer.class);
		float randDistributionFillRate = p_arguments.getArgument(ARG_ITEM_COUNT_RAND_FILL_RATE).getValue(Float.class);
		
		prepareTable();
		
		//main(itemCount, threads, randDistributionFillRate);
		mainEval(threads);
		
		System.out.println(m_tables.toCsv(true, "\t"));
		
		return 0;
	}
	
	/**
	 * Execute a full evaluation with a range of parameters.
	 * @param p_threads Number of threads for multi threaded part.
	 */
	private void mainEval(final int p_threads)
	{
		for (int i = 100; i <= 1000000; i *= 10)
		{
			// don't use a for loop, because floating point arithmetic
			// causes rounding issues
			main(i, p_threads, 0.1f);
			main(i, p_threads, 0.2f);
			main(i, p_threads, 0.3f);
			main(i, p_threads, 0.4f);
			main(i, p_threads, 0.5f);
			main(i, p_threads, 0.6f);
			main(i, p_threads, 0.7f);
			main(i, p_threads, 0.8f);
			main(i, p_threads, 0.9f);
			main(i, p_threads, 1.0f);
		}
	}
	
	/**
	 * Execute a single evaluation pass.
	 * @param p_itemCount Number of max items for a frontier.
	 * @param p_threads Number of threads to use for multi threaded section.
	 * @param p_randDistFillRate Distribution/Fill rate of of items in a frontier. 0.8 means that a frontier 
	 * 			will be filled with 80% of p_itemCount elements.
	 */
	private void main(final int p_itemCount, final int p_threads, final float p_randDistFillRate)
	{
		System.out.println("=======================================================================");
		System.out.println("Creating test data, totalItemCount " + p_itemCount + ", fill rate " + p_randDistFillRate);
		long[] testData = createTestData(p_itemCount, p_randDistFillRate);
		long testVector = createTestVector(testData);
		
		if (!doSingleThreaded(p_itemCount, testData, testVector, "SingleThreaded/" + p_itemCount, Float.toString(p_randDistFillRate))) {
			return;
		}
		
//		if (!doMultiThreaded(p_itemCount, p_threads, testData, testVector, "MultiThreaded/" + p_itemCount, Float.toString(p_randDistFillRate))) {
//			return;
//		}
		
		System.out.println("Execution done.");
		System.out.println("--------------------------");
	}
	
	/**
	 * Prepare the table for recording data.
	 */
	private void prepareTable()
	{
		m_tables = new EvaluationTables(12, 8, 10);
		m_tables.setTableName(0, "SingleThreaded/100/pushBack");
		m_tables.setTableName(1, "SingleThreaded/1000/pushBack");
		m_tables.setTableName(2, "SingleThreaded/10000/pushBack");
		m_tables.setTableName(3, "SingleThreaded/100000/pushBack");
		m_tables.setTableName(4, "SingleThreaded/1000000/pushBack");
		m_tables.setTableName(5, "SingleThreaded/100/popFront");
		m_tables.setTableName(6, "SingleThreaded/1000/popFront");
		m_tables.setTableName(7, "SingleThreaded/10000/popFront");
		m_tables.setTableName(8, "SingleThreaded/100000/popFront");
		m_tables.setTableName(9, "SingleThreaded/1000000/popFront");
		
		m_tables.setIntersectTopCornerNames("DataStructure");
		
		m_tables.setColumnNames(0, "0.1");
		m_tables.setColumnNames(1, "0.2");
		m_tables.setColumnNames(2, "0.3");
		m_tables.setColumnNames(3, "0.4");
		m_tables.setColumnNames(4, "0.5");
		m_tables.setColumnNames(5, "0.6");
		m_tables.setColumnNames(6, "0.7");
		m_tables.setColumnNames(7, "0.8");
		m_tables.setColumnNames(8, "0.9");
		m_tables.setColumnNames(9, "1.0");

		m_tables.setRowNames(0, "BulkFifoNaive");
		m_tables.setRowNames(1, "BulkFifo");
		m_tables.setRowNames(2, "TreeSetFifo");
		m_tables.setRowNames(3, "BitVector");
		m_tables.setRowNames(4, "BitVectorWithStartPos");
		m_tables.setRowNames(5, "BitVectorMultiLevel");
		m_tables.setRowNames(6, "HalfConcurrentBitVector");
		m_tables.setRowNames(7, "ConcurrentBitVector");
	}
	
	/**
	 * Prepare the data structures to be tested on the single test pass.
	 * @param p_itemCount Number of max items for a frontier.
	 * @return List of frontier lists to be executed on the single thread pass.
	 */
	private ArrayList<FrontierList> prepareTestsSingleThreaded(final long p_itemCount)
	{
		ArrayList<FrontierList> list = new ArrayList<FrontierList>();
		
		list.add(new BulkFifoNaive());
		list.add(new BulkFifo());
		list.add(new TreeSetFifo());
		list.add(new BitVector(p_itemCount));
		list.add(new BitVectorWithStartPos(p_itemCount));
		list.add(new BitVectorMultiLevel(p_itemCount));
		list.add(new HalfConcurrentBitVector(p_itemCount));
		list.add(new ConcurrentBitVector(p_itemCount));
		
		return list;
	}
	
	/**
	 * Prepare the data structures to be tested on the multi thread test pass.
	 * @param p_itemCount Number of max items for a frontier.
	 * @return List of frontier lists to be executed on the multi thread pass.
	 */
	private ArrayList<FrontierList> prepareTestsMultiThreaded(final long p_itemCount)
	{
		ArrayList<FrontierList> list = new ArrayList<FrontierList>();

		list.add(new ConcurrentBitVector(p_itemCount));
		
		return list;
	}

	/**
	 * Execute the test of one data structure single threaded.
	 * @param p_frontierList Frontier list to test/benchmark.
	 * @param testData Test data to be used.
	 * @param p_table Name of the table to put the recorded data into.
	 * @param p_column Name of the column in the table to put recorded data into.
	 * @return Test vector to verify if test data was successfully written and read back.
	 */
	private long executeTestSingleThreaded(final FrontierList p_frontierList, final long[] testData, final String p_table, final String p_column) {
		Stopwatch stopWatch = new Stopwatch();
				
		System.out.println("Pushing back data...");
		{
			stopWatch.start();
			for (int i = 0; i < testData.length; i++) {
				p_frontierList.pushBack(testData[i]);
			}
			stopWatch.stop();
			m_tables.set(p_table + "/pushBack", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack", MS_PRINT_READABLE_TIME);
		}
		System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) testData.length);
		
		System.out.println("Popping data front...");
		long vals = 0;
		{
			stopWatch.start();
			for (long i = 0; i < testData.length; i++) {
				vals += p_frontierList.popFront();
			}
			stopWatch.stop();
			m_tables.set(p_table + "/popFront", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront", MS_PRINT_READABLE_TIME);
		}

		return vals;
	}
	
	/**
	 * Execute the test of one data structure multi threaded.
	 * @param p_frontierList Frontier list to test/benchmark.
	 * @param testData Test data to be used.
	 * @param p_table Name of the table to put the recorded data into.
	 * @param p_column Name of the column in the table to put recorded data into.
	 * @return Test vector to verify if test data was successfully written and read back.
	 */
	private long executeTestMultiThreaded(final FrontierList p_frontierList, final int p_threadCount, final long[] testData, final String p_table, final String p_column) {
		Stopwatch stopWatch = new Stopwatch();
		
		System.out.println("Pushing back data...");
		{
			PushWorkerThread[] threads = new PushWorkerThread[p_threadCount];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new PushWorkerThread();
				threads[i].m_frontier = p_frontierList;
				threads[i].m_testData = new long[testData.length / threads.length];
				System.arraycopy(testData, i * (testData.length / threads.length), threads[i].m_testData, 0, threads[i].m_testData.length);				
				threads[i].start();
			}
			
			// wait a moment to ensure all threads are started
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			stopWatch.start();
			for (int i = 0; i < threads.length; i++) {
				threads[i].m_wait = false;
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			stopWatch.stop();
			m_tables.set(p_table + "/pushBack", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " pushBack", MS_PRINT_READABLE_TIME);
		}
		System.out.println("Data pushed, ratio added items/total items: " + p_frontierList.size() / (float) testData.length);
		
		System.out.println("Popping data front...");
		long val = 0;
		{
			PopWorkerThread[] threads = new PopWorkerThread[p_threadCount];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new PopWorkerThread();
				threads[i].m_frontier = p_frontierList;
				threads[i].start();
			}
			
			stopWatch.start();
			for (int i = 0; i < threads.length; i++) {
				threads[i].m_wait = false;
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
					val += threads[i].m_val;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			stopWatch.stop();
			m_tables.set(p_table + "/popFront", p_frontierList.getClass().getSimpleName(), p_column, stopWatch.getTimeStr());
			stopWatch.print(p_frontierList.getClass().getSimpleName() + " popFront", MS_PRINT_READABLE_TIME);
		}
		
		return val;
	}
	
	/**
	 * Do a full single thread pass with given parameters on all prepared frontiers.
	 * @param p_itemCount Max number of items for a single frontier.
	 * @param p_testData Test data to be used.
	 * @param p_testVector Test vector of the test data for verification.
	 * @param p_table Name of the table to put the recorded data into.
	 * @param p_column Name of the column in the table to put recorded data into.
	 * @return True if execution was successful and validation ok, false otherwise.
	 */
	private boolean doSingleThreaded(final int p_itemCount, final long[] p_testData, final long p_testVector, final String p_table, final String p_column) {
		System.out.println("---------------------------------------------------");
		System.out.println("Single threaded tests");
		ArrayList<FrontierList> frontiersToTest = prepareTestsSingleThreaded(p_itemCount);
		
		for (FrontierList frontier : frontiersToTest) {
			System.out.println("-------------");
			System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
			long resVector = executeTestSingleThreaded(frontier, p_testData, p_table, p_column);
			
			System.out.println("Data validation...");
			if (resVector != p_testVector) {
				System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Do a full multi thread pass with given parameters on all prepared frontiers.
	 * @param p_itemCount Max number of items for a single frontier.
	 * @param p_testData Test data to be used.
	 * @param p_testVector Test vector of the test data for verification.
	 * @param p_table Name of the table to put the recorded data into.
	 * @param p_column Name of the column in the table to put recorded data into.
	 * @return True if execution was successful and validation ok, false otherwise.
	 */
	private boolean doMultiThreaded(final int p_itemCount, final int p_threadCount, final long[] p_testData, final long p_testVector, final String p_table, final String p_column) {
		System.out.println("---------------------------------------------------");
		System.out.println("Multi threaded tests, threads: " + p_threadCount);
		ArrayList<FrontierList> frontiersToTest = prepareTestsMultiThreaded(p_itemCount);
		
		for (FrontierList frontier : frontiersToTest) {
			System.out.println("Testing frontier " + frontier.getClass().getSimpleName());
			long resVector = executeTestMultiThreaded(frontier, p_threadCount, p_testData, p_table, p_column);
			
			System.out.println("Data validation...");
			if (resVector != p_testVector) {
				System.out.println("ERROR: validation of " + frontier.getClass().getSimpleName() + " failed: " + resVector + " != " + p_testVector);
				//return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Create the test data with given parameters.
	 * @param p_totalItemCount Max number of items for the test data.
	 * @param p_randDistFillRate Distribution/Fill rate for the test data.
	 * @return Array with shuffled test data.
	 */
	private long[] createTestData(final int p_totalItemCount, final float p_randDistFillRate)
	{
		long[] testData;
		if (p_randDistFillRate < 1.0f && p_randDistFillRate > 0.0f) {
			System.out.println("Creating random distribution test data...");
			
			Random rand = new Random();
			TreeSet<Long> set = new TreeSet<Long>();
			testData = new long[(int) (p_totalItemCount * p_randDistFillRate)];
			int setCount = 0;

			// use a set to ensure generating non duplicates until we hit the specified fill rate
			while (((float) setCount / p_totalItemCount) < p_randDistFillRate)
			{
				if (set.add((long) (rand.nextFloat() * p_totalItemCount))) {
					setCount++;
				}
			}

			for (int i = 0; i < testData.length; i++) {
				testData[i] = set.pollFirst();
			}
			shuffleArray(testData);
		} else {
			System.out.println("Creating continous test data...");
			
			testData = new long[p_totalItemCount];
			for (int i = 0; i < testData.length; i++) {
				testData[i] = i;
			}
		}
		return testData;
	}
	
	/**
	 * Shuffle the contents of an array.
	 * @param p_array Array with contents to shuffle.
	 */
	private static void shuffleArray(final long[] p_array) {
		Random rnd = new Random();
		for (int i = p_array.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			long a = p_array[index];
			p_array[index] = p_array[i];
			p_array[i] = a;
		}
	}
	
	/**
	 * Create the test vector for verification.
	 * @param p_testData Test data to create the test vector of.
	 * @return Test vector.
	 */
	private long createTestVector(final long[] p_testData) {
		long testVec = 0;
		for (int i = 0; i < p_testData.length; i++) {
			testVec += p_testData[i];
		}
		return testVec;
	}
	
	/**
	 * Thread for multi thread pass to push back the data concurrently.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 *
	 */
	private static class PushWorkerThread extends Thread
	{
		public FrontierList m_frontier = null;
		public long[] m_testData = null;
		public volatile boolean m_wait = true;
		
		@Override
		public void run()
		{
			while (m_wait)
			{
				// busy loop to avoid latency on start
			}
			
			for (int i = 0; i < m_testData.length; i++) {
				m_frontier.pushBack(m_testData[i]);
			}
		}
	}
	
	/**
	 * Thread for multi thread pass to pop the data from the front concurrently.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 *
	 */
	private static class PopWorkerThread extends Thread
	{
		public FrontierList m_frontier = null;
		public volatile long m_val = 0;
		public volatile boolean m_wait = true;
		
		@Override
		public void run()
		{
			while (m_wait)
			{
				// busy loop to avoid latency on start
			}
			
			long val = 0;
			while (true)
			{
				long tmp = m_frontier.popFront();
				if (tmp == -1) {
					break;
				}
				val += tmp;
			}
			
			m_val = val;
		}
	}
}
