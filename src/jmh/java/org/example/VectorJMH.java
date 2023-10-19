package org.example;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

@State(Scope.Benchmark)
public class VectorJMH {
  static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  int[] numbers = new int[1024 * 1024];
  boolean[] mask = new boolean[1024 * 1024];

  @Setup
  public void setup() {
    Random random = new Random();
    for (int i = 0; i < numbers.length; i++) {
      numbers[i] = random.nextInt();
      mask[i] = random.nextBoolean();
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
  public void vectorSum(Blackhole bh) {
    long sum = 0;
    for (int i = 0; i < numbers.length; i += SPECIES.length()) {
      if (numbers.length - i < SPECIES.length()) {
        VectorMask<Integer> vm = SPECIES.indexInRange(i, Math.min(numbers.length - i, SPECIES.length()));
        IntVector v = IntVector.fromArray(SPECIES, numbers, i, vm);
        sum += v.reduceLanes(VectorOperators.ADD, vm);
      } else {
        IntVector v = IntVector.fromArray(SPECIES, numbers, i);
        sum += v.reduceLanes(VectorOperators.ADD);
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
      sum += v.reduceLanes(VectorOperators.ADD, vm);
    }
    bh.consume(sum);
  }
}
