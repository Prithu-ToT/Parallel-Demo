package Parallel;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class RadixSortTest {
    public static void main(String[] args) {
        System.out.println("Test sort program");

        Random rand = new Random();
        int[] ara = new int[100_000_000];
        for (int i = 0; i < ara.length; i++) {
            ara[i] = rand.nextInt();
        }

        Instant start = Instant.now();
        IntegerSorter is = new IntegerSorter(ara);
        is.radixSort(0, ara.length);
        Instant end = Instant.now();
        Duration runtime = Duration.between(start, end);

        for (int i = 1; i < ara.length; i++) {
            if(ara[i] < ara[i-1]) {
                System.out.println("FUCKING FUCK FUCKED UP SORTS");
                break;
            }
            else if(i==ara.length-1)
                System.out.println("Win");
        }

        System.out.println();
        System.out.println("Time taken : " + runtime.toMillis());

    }
}
