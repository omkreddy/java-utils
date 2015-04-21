/** StripedExecutorTest.java -
 * @version      $Name$
 * @module       com.nmsworks.cygnet.util
 * 
 * @purpose
 * @see
 *
 * @author   Manikumar Reddy (kumar@nmsworks.co.in)
 *
 * @created  17-Mar-2015
 * $Id$
 *
 * @bugs
 *
 * Copyright 2012-2013 NMSWorks Software Pvt Ltd. All rights reserved.
 * NMSWorks PROPRIETARY/CONFIDENTIAL. Use is subject to licence terms.
 */

package utils.concurrency;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import utils.concurrency.StripedExecutor;
import utils.concurrency.StripedRunnable;

public class StripedExecutorTest {

    @Before
    public void initialize() {
        TestRunnable.outOfSequence =
                TestUnstripedRunnable.outOfSequence =
                        TestFastRunnable.outOfSequence = false;
    }
 

   @Test
    public void testSingleStripeRunnable() throws InterruptedException {
        StripedExecutor pool = new StripedExecutor(10);
        Object stripe = new Object();
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 1000; i++) {
            pool.execute(new TestRunnable(stripe, actual, i));
        }
   
        assertFalse(pool.isTerminated());
        pool.shutdown(true);
        assertTrue(pool.awaitTermination());
        assertFalse("Expected no out-of-sequence runnables to execute",
                TestRunnable.outOfSequence);
        assertTrue(pool.isTerminated());
        System.out.println("Completed");
    }


    @Test
    public void testUnstripedRunnable() throws InterruptedException {
        StripedExecutor pool = new StripedExecutor(10);
        AtomicInteger actual = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            pool.execute(new TestUnstripedRunnable(actual, i));
        }
        pool.shutdown(true);
        assertTrue(pool.awaitTermination());
        System.out.println(TestUnstripedRunnable.outOfSequence);
        assertTrue("Expected at least some out-of-sequence runnables to execute",
                TestUnstripedRunnable.outOfSequence);
    }

    @Test
    public void testMultipleStripes() throws InterruptedException {
        final StripedExecutor pool = new StripedExecutor(10);
        ExecutorService producerPool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            producerPool.submit(new Runnable() {
                public void run() {
                    Object stripe = new Object();
                    AtomicInteger actual = new AtomicInteger(0);
                    for (int i = 0; i < 100; i++) {
                        pool.execute(new TestRunnable(stripe, actual, i));
                    }
                }
            });
        }
        producerPool.shutdown();

        while (!producerPool.awaitTermination(1, TimeUnit.MINUTES)) ;

        pool.shutdown(true);
        assertTrue(pool.awaitTermination());
        assertFalse("Expected no out-of-sequence runnables to execute",
                TestRunnable.outOfSequence);
    }
    

    @Test
    public void testMultipleFastStripes() throws InterruptedException {
        final StripedExecutor pool = new StripedExecutor(10);
        ExecutorService producerPool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            producerPool.submit(new Runnable() {
                public void run() {
                    Object stripe = new Object();
                    AtomicInteger actual = new AtomicInteger(0);
                    for (int i = 0; i < 100; i++) {
                        pool.execute(new TestFastRunnable(stripe, actual, i));
                    }
                }
            });
        }
        producerPool.shutdown();

        while (!producerPool.awaitTermination(1, TimeUnit.MINUTES)) ;

        pool.shutdown(true);
        assertTrue(pool.awaitTermination());
        assertFalse("Expected no out-of-sequence runnables to execute",
                TestFastRunnable.outOfSequence);
    }

	public static class TestRunnable implements StripedRunnable {
        private final Object stripe;
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence = false;

        public TestRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
            this.stripe = stripe;
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public void run() {
            try {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                Thread.sleep(rand.nextInt(10) + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            System.out.printf("Execute strip %h %d %d%n", stripe, actual, expected);
            assertEquals("out of sequence", actual, expected);
        }

		@Override
		public Object getKey() {
			return stripe;
		}
    }
    
    public static class TestFastRunnable implements StripedRunnable {
        private final Object stripe;
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence = false;

        public TestFastRunnable(Object stripe, AtomicInteger stripeSequence, int expected) {
            this.stripe = stripe;
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public Object getStripe() {
            return stripe;
        }

        public void run() {
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            System.out.printf("Execute strip %h %d %d%n", stripe, actual, expected);
            assertEquals("out of sequence", actual, expected);
        }

		@Override
		public Object getKey() {
			return stripe;
		}
    }

    public static class TestUnstripedRunnable implements Runnable {
        private final AtomicInteger stripeSequence;
        private final int expected;
        private static volatile boolean outOfSequence = false;

        public TestUnstripedRunnable(AtomicInteger stripeSequence, int expected) {
            this.stripeSequence = stripeSequence;
            this.expected = expected;
        }

        public void run() {
            try {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                Thread.sleep(rand.nextInt(10) + 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int actual = stripeSequence.getAndIncrement();
            if (actual != expected) {
                outOfSequence = true;
            }
            System.out.println("Execute unstriped " + actual + ", " + expected);
        }
    }
}

/**
 * $Log$
 * 
 */
