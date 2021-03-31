package edu.coursera.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value=0;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        static long qwe=5000;

        @Override
        protected void compute() {



            if (startIndexInclusive-endIndexExclusive <= qwe){
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) { value += 1 / input[i];}
            } else {
                ReciprocalArraySumTask left = new ReciprocalArraySumTask(startIndexInclusive, (startIndexInclusive+endIndexExclusive)/2,input);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask((startIndexInclusive+endIndexExclusive)/2,endIndexExclusive,input);
                left.fork();
                right.compute();
                left.join();
                value = left.value+ right.value;
            }
        }
    }
    /*
    private static class SumArray extends RecursiveAction {
        static int qwe=100000000;
        int q;
        int w;
        double[] arr;
        double ans = 0;

        SumArray(double[] a, int l, int h){
            q=l;
            w=h;
            arr=a;
        }

        @Override
        protected void compute() {
            if (q-w <= qwe){
                for (int i = q; i < w; i++) { ans += 1 / arr[i];}
            } else {
                SumArray left = new SumArray(arr, q, (q+w)/2);
                SumArray right = new SumArray(arr,(q+w)/2,w);
                left.fork();
                right.compute();
                left.join();
                ans = left.ans+ right.ans;
            }
        }
    }

     */

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */

    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;
        final int q=(input.length/2);
        double sum = 0;
        //System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism","1");


        ReciprocalArraySumTask t1 = new ReciprocalArraySumTask(0,q,input);
        ReciprocalArraySumTask t2 = new ReciprocalArraySumTask(q,input.length,input);
        ReciprocalArraySumTask.invokeAll(t1,t2);
        t1.join();
        t2.join();
        sum=t1.value+ t2.value;
        //sum=t1.getValue()+t2.getValue();


        /*
        ReciprocalArraySumTask t = new ReciprocalArraySumTask(0,input.length,input);
        ForkJoinPool.commonPool().invoke(t);
        sum=t.getValue();

         */





        /*
        ForkJoinTask<Double> t1=new ForkJoinTask<Double>() {
            @Override
            public Double getRawResult() {
                double locsum = 0;
                for (int i = 0; i < q; i++) {
                locsum += 1 / input[i];}
                return locsum;
            }

            @Override
            protected void setRawResult(Double value) {

            }

            @Override
            protected boolean exec() {
                return true;
            }
        };
        ForkJoinTask<Double> t2= new ForkJoinTask<Double>() {
            @Override
            public Double getRawResult() {
                double locsum = 0;
                for (int j = q; j < input.length; j++) {
                    locsum += 1 / input[j];}
                return locsum;
            }

            @Override
            protected void setRawResult(Double value) {

            }

            @Override
            protected boolean exec() {
                return true;
            }
        };
*/
        //for (int i = 0; i < input.length; i++) { sum += 1 / input[i]; }

        //t1.fork();sum+=t2.invoke();sum+=t1.join();

        return sum;

    }



    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        double sum = 0;



        //ForkJoinPool pool = new ForkJoinPool(numTasks);




        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }
}
