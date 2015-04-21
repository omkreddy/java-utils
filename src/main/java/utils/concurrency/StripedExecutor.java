package utils.concurrency;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The StripedExecutor accepts StripedRunnable instances and make sure the tasks
 * submitted with the same key/stripe will be executed in the same order as task
 * submission (order of calling the {@link #execute(Runnable)} method).
 * 
 * StripedRunnable implementations must return keys with correct hashCode and
 * equals methods
 * 
 * If there are more than one thread in the given {@link Executor}, tasks
 * submitted with different keys may be executed in parallel, but never for
 * tasks submitted with the same key.
 * 
 * It is advisable to use separate wrapped Executor for each StripedExecutpor.
 * 
 * <p/>
 * In this version, submitted tasks do not necessarily have to implement the
 * StripedRunnable interface. If they do not, then they will simply be passed
 * onto the wrapped ExecutorService directly.
 * <p/>
 * 
 * This code is borrowed/inspired from hazelcast opensource project and also from
 * http://www.javaspecialists.eu/archive/Issue206.html
 * <p/>
 * 
 */

public final class StripedExecutor implements Executor {

	/**
	 * The wrapped ExecutorService that will actually execute our tasks.
	 */
	private final ExecutorService executor;

	/**
	 * No. Stripes for this executor
	 */
	private final int stripes;

	/**
	 * No. of worker queues. These are equivalent to no. of stripes
	 */
	private final Worker[] workers;

	private int maximumQueueSize;

	private volatile boolean terminated = false;
	private volatile boolean shutDownExecutor = false;

	/**
	 * StripedExecutpor with given executor and number of. stripes
	 * 
	 * @param executor
	 * @param stripes
	 */
	public StripedExecutor(ExecutorService executor, int stripes) {
		this(executor, stripes, Integer.MAX_VALUE);
	}

	/**
	 * StripedExecutpor with same number of. stripes and threads
	 * 
	 * @param executor
	 * @param stripes
	 */
	public StripedExecutor(int stripes) {
		this(Executors.newCachedThreadPool(), stripes, Integer.MAX_VALUE);
	}

	/**
	 * StripedExecutpor with given threadPool size and number of. stripes
	 * 
	 * @param executor
	 * @param stripes
	 */
	public StripedExecutor(int threadPoolSize, int stripes) {
		this(Executors.newFixedThreadPool(threadPoolSize), stripes, Integer.MAX_VALUE);
	}

	/**
	 * StripedExecutpor with given executor and number of. stripes and maximum
	 * queue size for each stripe
	 * 
	 * @param executor
	 * @param stripes
	 * @param maximumQueueSize
	 */
	public StripedExecutor(ExecutorService executor, int stripes,
			int maximumQueueSize) {
		if (stripes <= 0)
			throw new IllegalArgumentException(
					"Stripes must be positive number");
		this.maximumQueueSize = maximumQueueSize;
		this.stripes = stripes;
		this.executor = executor;
		workers = new Worker[stripes];
		for (int i = 0; i < stripes; i++) {
			workers[i] = new Worker();
		}
	}

	/**
	 * getQueueSize -- returns size across all the worker queues
	 *    @return int
	 *    @see  
	 *    @bugs 
	 */
	public int getQueueSize() {
		int size = 0;
		for (Worker worker : workers) {
			size += worker.workQueue.size();
		}
		return size;
	}
	
	/**
	 * getWorkerQueueSize -- returns queueSize for each worker queue.
	 *    @return Map<String,Integer>
	 *    @see  
	 *    @bugs 
	 */
	public Map<String, Integer> getWorkerQueueSize() {
		Map<String, Integer> map = new HashMap<String, Integer>();
		int i = 0, size = 0;
		for (Worker worker : workers) {
			map.put("Worker"+(i++), worker.workQueue.size());
			size += worker.workQueue.size();
		}
		map.put("Total", size);
		return map;
	}

	/**
	 * If the given Runnable is of type StripedRunnable, then the Runnable is
	 * executed in the Striped order. else it will be passed to underlying
	 * executor..
	 * 
	 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
	 */
	@Override
	public void execute(Runnable command) {

		if (terminated) {
			throw new RejectedExecutionException("Executor is terminated!");
		}

		if (command instanceof StripedRunnable) {
			final Object key = ((StripedRunnable) command).getKey();

			if (key == null) {
				executor.execute(command);
				return;
			}

			final int index = Math.abs(key.hashCode()) % stripes;
			workers[index].execute(command);
		} else
			executor.execute(command);
	}

	/**
	 * shutdown -- Shuts down the StripedExecutor. No more tasks will be
	 * submitted.
	 * 
	 * @return void
	 * @see
	 * @bugs
	 */
	public void shutdown(boolean stopUnderlyingExecutor) {
		terminated = true;
		if (stopUnderlyingExecutor)
			shutDownExecutor = true;
	}

	/**
	 * shutdownNow --Shuts down the StripedExecutor. No more tasks will be
	 * submitted.
	 * 
	 * @return void
	 * @see
	 * @bugs
	 */
	public void shutdownNow(boolean stopUnderlyingExecutor) {
		shutdown(stopUnderlyingExecutor);

		for (Worker worker : workers) {
			worker.workQueue.clear();
		}

		if (stopUnderlyingExecutor)
			executor.shutdownNow();
	}

	/**
	 * waits till all the submitted tasks completion
	 * 
	 */
	public boolean awaitTermination() throws InterruptedException {
		if (terminated == false)
			return false;

		for (Worker worker : workers) {
			while (worker.workQueue.peek() != null) {
				Thread.sleep(100);
			}
		}

		if (shutDownExecutor) {
			executor.shutdown();
			while (!executor.awaitTermination(1, TimeUnit.HOURS));
		}
		return true;
	}

	/**
	 * Returns true if shutdown() or shutdownNow() have been called; false
	 * otherwise.
	 */
	public boolean isShutdown() {
		return terminated == true;
	}

	/**
	 * Returns true if this pool has been terminated, that is, all the worker
	 * queue are empty and the wrapped ExecutorService has been terminated.
	 */
	public boolean isTerminated() {
		if (terminated == false)
			return false;
		for (Worker worker : workers) {
			if (!worker.workQueue.isEmpty())
				return false;
		}
		return true;
	}

	private class Worker implements Executor, Runnable {

		private final AtomicBoolean scheduled = new AtomicBoolean(false);

		private final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(
				maximumQueueSize);

		public void execute(Runnable command) {
			boolean offered = workQueue.offer(command);

			if (!offered) {
				throw new RejectedExecutionException("Worker queue is full!");
			}

			schedule();
		}

		private void schedule() {
			// if it is already scheduled, we don't need to schedule it again.
			if (scheduled.get()) {
				return;
			}

			if (!workQueue.isEmpty() && scheduled.compareAndSet(false, true)) {
				try {
					executor.execute(this);
				} catch (RejectedExecutionException e) {
					scheduled.set(false);
					throw e;
				}
			}
		}

		public void run() {
			try {
				Runnable r;
				do {
					r = workQueue.poll();
					if (r != null) {
						try {
							r.run();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} while (r != null);
			} finally {
				scheduled.set(false);
				schedule();
			}
		}
	}
}
