package org.example;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;
import java.util.Random;

/**
 * Testing time aggregations.
 * 
 * WARNING: Watch for rollover with the ints!
 *
 * VectorIntSingleLaneAggregations.arraySum                       avgt    5    101.113 ±   4.457  us/op
 * VectorIntSingleLaneAggregations.arraySumWithMask               avgt    5   3295.850 ± 153.746  us/op
 * VectorIntSingleLaneAggregations.arraySumWithMaskBitmap         avgt    5   3946.306 ± 662.679  us/op
 * VectorIntSingleLaneAggregations.arraySumWithMaskRoaringBitmap  avgt    5  15508.068 ± 878.481  us/op
 * VectorIntSingleLaneAggregations.vectorSum                      avgt    5    208.797 ±  17.983  us/op
 * VectorIntSingleLaneAggregations.vectorSumWithMask              avgt    5    293.366 ±  80.278  us/op
 */
@State(Scope.Benchmark)
public class VectorIntSingleLaneAggregations {
  static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  int[] numbers = new int[1024 * 1024];
  boolean[] mask = new boolean[1024 * 1024];
  BitSet bitSet = new BitSet();
  RoaringBitmap rb = new RoaringBitmap();

  @Setup
  public void setup() {
    Random random = new Random();
    for (int i = 0; i < numbers.length; i++) {
      numbers[i] = random.nextInt(0, 4096);
      mask[i] = random.nextBoolean();
      bitSet.set(i, mask[i]);
      if (mask[i]) rb.add(i);
    }
  }

  @Benchmark
  public void arraySum(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i++) {
      sum += numbers[i];
    }
    bh.consume(sum);
  }

  @Benchmark
  public void arraySumWithMask(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i++) {
      if (mask[i])
        sum += numbers[i];
    }
    bh.consume(sum);
  }

  @Benchmark
  public void arraySumWithMaskBitmap(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i++) {
      if (bitSet.get(i))
        sum += numbers[i];
    }
    bh.consume(sum);
  }

  @Benchmark
  public void arraySumWithMaskRoaringBitmap(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i++) {
      if (rb.contains(i))
        sum += numbers[i];
    }
    bh.consume(sum);
  }

  @Benchmark
  public void vectorSum(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i += SPECIES.length()) {
      if (numbers.length - i < SPECIES.length()) {
        VectorMask<Integer> vm = SPECIES.indexInRange(i, numbers.length - i);
        IntVector v = IntVector.fromArray(SPECIES, numbers, i, vm);
        sum += v.reduceLanesToLong(VectorOperators.ADD, vm);
      } else {
        IntVector v = IntVector.fromArray(SPECIES, numbers, i);
        sum += v.reduceLanesToLong(VectorOperators.ADD);
      }
    }
    bh.consume(sum);
  }

  @Benchmark
  public void vectorSumWithMask(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i += SPECIES.length()) {
      VectorMask<Integer> vm = SPECIES.loadMask(mask, i);
      IntVector v = IntVector.fromArray(SPECIES, numbers, i, vm);
      sum += v.reduceLanesToLong(VectorOperators.ADD, vm);
    }
    bh.consume(sum);
  }

  public static void main(String[] args) {
    VectorIntSingleLaneAggregations v = new VectorIntSingleLaneAggregations();
    v.setup();
    Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    v.arraySum(bh);
    v.arraySumWithMask(bh);
    v.vectorSum(bh);
    v.vectorSumWithMask(bh);
  }
}
