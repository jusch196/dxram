
package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Thread safe, lock free implementation of a frontier listed based on
 * a bit vector.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class ConcurrentBitVector implements FrontierList {
	private long m_maxElementCount;
	private AtomicLongArray m_vector;

	private AtomicLong m_itPos = new AtomicLong(0);
	private AtomicLong m_count = new AtomicLong(0);
	private AtomicLong m_inverseCount = new AtomicLong(0);

	/**
	 * Constructor
	 *
	 * @param p_maxElementCount Specify the maximum number of elements.
	 */
	public ConcurrentBitVector(final long p_maxElementCount) {
		m_maxElementCount = p_maxElementCount;
		m_vector = new AtomicLongArray((int) ((p_maxElementCount / 64L) + 1L));
		m_inverseCount.set(m_maxElementCount);
	}

	public static void main(final String[] p_args) throws Exception {
		final int vecSize = 10000000;
		ConcurrentBitVector vec = new ConcurrentBitVector(vecSize);

		Thread[] threads = new Thread[24];
		while (true) {
			System.out.println("--------------------------");
			System.out.println("Fill....");
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new Thread() {
					@Override
					public void run() {
						Random rand = new Random();

						for (int i = 0; i < 100000; i++) {
							vec.pushBack(rand.nextInt(vecSize));
						}
					}
				};
				threads[i].start();
			}

			for (Thread thread : threads) {
				thread.join();
			}

			System.out.println("Total elements: " + vec.size());
			System.out.println("Empty...");

			AtomicLong sum = new AtomicLong(0);
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new Thread() {
					private long m_count;

					@Override
					public void run() {
						while (true) {
							long elem = vec.popFront();
							if (elem == -1) {
								sum.addAndGet(m_count);
								break;
							}

							m_count++;
						}
					}
				};
				threads[i].start();
			}

			for (Thread thread : threads) {
				thread.join();
			}

			System.out.println("Empty elements " + vec.size() + ", total elements got " + sum.get());

			vec.reset();
		}
	}

	@Override
	public boolean pushBack(final long p_index) {
		long tmp = 1L << (p_index % 64L);
		int index = (int) (p_index / 64L);

		while (true) {
			long val = m_vector.get(index);
			if ((val & tmp) == 0) {
				if (!m_vector.compareAndSet(index, val, val | tmp)) {
					continue;
				}
				m_count.incrementAndGet();
				m_inverseCount.decrementAndGet();
				return true;
			}

			return false;
		}
	}

	@Override
	public boolean contains(final long p_val) {
		long tmp = 1L << (p_val % 64L);
		int index = (int) (p_val / 64L);
		return (m_vector.get(index) & tmp) != 0;
	}

	@Override
	public long capacity() {
		return m_maxElementCount;
	}

	@Override
	public long size() {
		return m_count.get();
	}

	@Override
	public boolean isEmpty() {
		return m_count.get() == 0;
	}

	@Override
	public void reset() {
		m_itPos.set(0);
		m_count.set(0);
		m_inverseCount.set(m_maxElementCount);
		for (int i = 0; i < m_vector.length(); i++) {
			m_vector.set(i, 0);
		}
	}

	@Override
	public long popFront() {
		while (true) {
			// this section keeps threads out
			// if the vector is already empty
			long count = m_count.get();
			if (count > 0) {
				if (!m_count.compareAndSet(count, count - 1)) {
					continue;
				}

				break;
			} else {
				return -1;
			}
		}

		long itPos = m_itPos.get();
		while (true) {
			try {
				if ((m_vector.get((int) (itPos / 64L)) & (1L << (itPos % 64L))) != 0) {
					if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
						itPos = m_itPos.get();
						continue;
					}

					return itPos;
				}

				if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
					itPos = m_itPos.get();
				}
			} catch (final IndexOutOfBoundsException e) {
				System.out.println("Exception: " + itPos + " / " + m_count.get());
				throw e;
			}
		}
	}

	public void resetInverse() {
		m_itPos.set(0);
		m_inverseCount.set(m_maxElementCount);
	}

	public long popFrontInverse() {
		while (true) {
			// this section keeps threads out
			// if the vector is already empty
			long count = m_inverseCount.get();
			if (count > 0) {
				if (!m_inverseCount.compareAndSet(count, count - 1)) {
					continue;
				}

				break;
			} else {
				return -1;
			}
		}

		long itPos = m_itPos.get();
		while (true) {
			try {
				if ((m_vector.get((int) (itPos / 64L)) & (1L << (itPos % 64L))) == 0) {
					if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
						itPos = m_itPos.get();
						continue;
					}

					return itPos;
				}

				if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
					itPos = m_itPos.get();
				}
			} catch (final IndexOutOfBoundsException e) {
				System.out.println("Exception: " + itPos + " / " + m_count.get());
				throw e;
			}
		}
	}

	@Override
	public String toString() {
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
	}
}