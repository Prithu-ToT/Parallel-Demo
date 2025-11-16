package Parallel;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IntegerSorter {

    private static final int BUCKET_COUNT = 256; // 256 for 1 byte
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int BYTE_LENGTH =  8;
    private static final int INT_DATA_LENGTH = 32;

    private int[] array;
    private final int[][] buckets;
    private final int[][] bucketEnds;
    private ExecutorService threadPool;
    int n;

    private class ParallelCounter {

        private final int start, end, threadID, shift;       // end is exlusive
        private int[] src, dst;

        public ParallelCounter(int shift, int[] src, int[] dst, int start, int end, int threadID) {
            this.shift = shift;
            this.start = start;
            this.end = end;
            this.threadID=threadID;
            this.src = src;
            this.dst = dst;
        }

        public void count() {

            // creating buckets
            Arrays.fill(buckets[threadID], 0);

            // looking for this data fits in which bucket
            for (int i = start; i < end; i++) {
                int value = (src[i] >>> shift) & 0xFF;
                buckets[threadID][value]++;
            }
        }

        public void redistribute(){

            for (int i = start; i < end; i++) {
                int value = (src[i] >>> shift) & 0xFF;        // extract digit or value from data
                int writePos;

                writePos = bucketEnds[threadID][value] - buckets[threadID][value];
                buckets[threadID][value]--;

                dst[writePos] = src[i];
            }

        }
    }

    public IntegerSorter(int[] array) {
        this.array = array;
        this.n  = array.length;
        threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
        buckets = new int[THREAD_COUNT][BUCKET_COUNT];
        bucketEnds = new int[THREAD_COUNT][BUCKET_COUNT];

    }

    private void countSort(int byteIndx, int[] src, int[] dst, int start, int length){

        int end = start+length;
        int chunkLength = length/THREAD_COUNT;
        int shift = byteIndx * BYTE_LENGTH;

        ParallelCounter[] counters =  new ParallelCounter[THREAD_COUNT];
        CountDownLatch counterRemaining = new CountDownLatch(THREAD_COUNT);

        int chunkStart = start, chunkEnd = start + chunkLength;
        for (int i = 0; i < THREAD_COUNT; i++) {
            counters[i] = new ParallelCounter(shift, src, dst, chunkStart, chunkEnd,i);
            chunkStart = chunkEnd;
            chunkEnd = (i==THREAD_COUNT-2)?end:(chunkStart+chunkLength);

            // sumbit counter to executor
            int finalI = i;
            threadPool.submit(()->{
               counters[finalI].count();
               counterRemaining.countDown();
            });

        }

        // wait for threads to stop working
        try {
            counterRemaining.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //calculate where bucket ends for each thread
        int cumulative = start;
        for (int b = 0; b < BUCKET_COUNT; b++) {
            for (int t = 0; t < THREAD_COUNT; t++) {
                bucketEnds[t][b] = cumulative + buckets[t][b];
                cumulative += buckets[t][b];
            }
        }

        CountDownLatch distributorRemaining = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            int finalI = i;
            threadPool.submit(()->{
                counters[finalI].redistribute();
                distributorRemaining.countDown();
            });

        }

        // wait for threads to stop working
        try {
            distributorRemaining.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void radixSort(int start, int length){        // Sorts in place

        int[] src = array;
        int[] dst = new int[array.length];
        int end = start + length;

        final int signBit = 1 << 31;
        for (int i = start; i < end; i++) src[i] ^= signBit;

        int totalPass = INT_DATA_LENGTH/BYTE_LENGTH;
        for (int pass = 0; pass < totalPass; pass++) {
            countSort(pass, src, dst, start, length);

            // swap the roles
            int[] tmp = src;
            src = dst;
            dst = tmp;
        }

        // if final result is in "src", but not in "array", copy once
        if (src != array)
            System.arraycopy(src, start, array, start, length);

        for (int i = start; i < end; i++)
            array[i] ^= signBit;
        threadPool.close();
    }
}
