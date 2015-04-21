package utils.concurrency;

/**
 * All of the Runnables with the same "Stripe/Key" will be executed serially.
 *
 */
public interface StripedRunnable extends Runnable {
	Object getKey();
}
