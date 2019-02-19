import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class AtomicRecordFetcher {

    public static class Record {
        // TODO placeholder
    }

    public AtomicReference<LazyReference<Record, IOException>> theLazyReference;

    public AtomicRecordFetcher() {
        // On initialization, create the first LazyReference
        takeLazyReference();
    }

    public Record nextImpl() {
        return null; // TODO placeholder
    }

    /** Get the current value of {@link #theLazyReference}, and then set the value to a new instance. */
    private LazyReference<Record, IOException> takeLazyReference() {
        // LazyReference is wrapped in AtomicReference so that resetting the LazyReference back to
        // to "uninitialized" (by replacing the previous LazyReference with a new instance) is an
        // atomic operation
        return theLazyReference.getAndSet(new LazyReference<Record, IOException>() {
            @Override
            public Record newInstance() throws IOException {
                // Make a single call to nextImpl() for each LazyReference instance
                return nextImpl();
            }
        });
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
        private final CountDownLatch initialized = new CountDownLatch(1);

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
                    initialized.countDown();
                }
            }
            // Wait for initialization to complete
            initialized.await();
            if (reference == null) {
                throw new NullPointerException("newInstance() returned null");
            }
            return reference;
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
        return theLazyReference.get().get();
    }

    public Record next() throws InterruptedException, IOException {
        // Either use the cached result of nextImpl(), or if peek() was not called since the
        // last call to next(), call nextImpl(). Then replace the LazyReference with a new one.
        return takeLazyReference().get();
    }

}
