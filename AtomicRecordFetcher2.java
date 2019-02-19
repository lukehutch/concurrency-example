import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Feel free to use this code freely, or license it however you want.
 * 
 * @author Luke Hutchison
 */
public class AtomicRecordFetcher2 {

    public static class Record {
        // TODO placeholder
    }

    public LazyReference<Record, IOException> theLazyReference = new LazyReference<Record, IOException>() {
        @Override
        public Record newInstance() throws IOException {
            return nextImpl();
        }
    };

    public Record nextImpl() throws IOException {
        return null; // TODO placeholder
    }

    /**
     * Assign a reference atomically, like {@link AtomicReference}, except that initialization of the reference
     * value is separated from assignment of the reference, and the reference value is generated lazily by calling
     * {@link #newInstance()} on the first call to {@link #get()}. All threads but the first getter will block on
     * the completion of execution of {@link #newInstance()} by the first getter.
     *
     * @param <V>
     *            the reference type
     * @param <E>
     *            an exception type that may be thrown by newInstance(), or {@link RuntimeException} if none.
     */
    public static abstract class LazyReference<V, E extends Exception> {
        private V reference;
        private final Semaphore firstGetter = new Semaphore(1);
        // Need to use AtomicReference for memory safety
        private final AtomicReference<CountDownLatch> initialized = new AtomicReference<CountDownLatch>();
        {
            initialized.set(new CountDownLatch(1));
        }

        /**
         * Get the singleton value, blocking until the first thread to call this method has finished calling
         * {@link #newInstance()}. Subsequently, this singleton value is returned without blocking.
         *
         * @return the singleton value.
         * @throws E
         *             if {@link #newInstance()} throws E.
         * @throws InterruptedException
         *             if the thread was interrupted while waiting for the value to be set.
         * @throws NullPointerException
         *             if {@link #newInstance()} returns null.
         */
        public V get() throws E, InterruptedException {
            if (firstGetter.tryAcquire()) {
                // This is the first thread that has called get()
                try {
                    // Initialize the reference by calling newInstance()
                    reference = newInstance();
                } finally {
                    // Release all getter threads
                    initialized.get().countDown();
                }
            }
            // Wait for initialization to complete
            initialized.get().await();
            if (reference == null) {
                throw new NullPointerException("newInstance() returned null");
            }
            return reference;
        }

        /**
         * Get the singleton value, blocking until the first thread to call {@link #get()} has finished calling
         * {@link #newInstance()}. Then set the reference value back to uninitialized, and return the value that was
         * returned by {@link #newInstance()}.
         *
         * @return the singleton value.
         * @throws E
         *             if {@link #newInstance()} throws E.
         * @throws InterruptedException
         *             if the thread was interrupted while waiting for the value to be set.
         * @throws NullPointerException
         *             if {@link #newInstance()} returns null.
         */
        public V take() throws E, InterruptedException {
            // Get the value of the reference
            final V value = get();
            // Reset the initialized state, then raise the firstGetter semaphore
            // (which is the reverse of the order they are checked in)
            initialized.set(new CountDownLatch(1));
            firstGetter.release();
            return value;
        }

        /**
         * Create a new instance of the reference type.
         *
         * @return the new instance (must not be null).
         * @throws E
         *             the e
         */
        public abstract V newInstance() throws E;
    }

    public Record peek() throws InterruptedException, IOException {
        // Call nextImpl() if peek() has not been called yet, or reuse the cached reference
        // if peek() has previously been called since the last next() call
        return theLazyReference.get();
    }

    public Record next() throws InterruptedException, IOException {
        // Either use the cached result of nextImpl(), or if peek() was not called since the
        // last call to next(), call nextImpl(). Then replace the LazyReference with a new one.
        return theLazyReference.take();
    }

}
