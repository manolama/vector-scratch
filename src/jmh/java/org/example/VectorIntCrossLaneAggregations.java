package org.example;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

@State(Scope.Benchmark)
public class VectorIntCrossLaneAggregations {
  static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  int[] seta = new int[1024 * 1024];
  int[] setb = new int[1024 * 1024];
  boolean[] mask = new boolean[1024 * 1024];

  @Setup
  public void setup() {
    Random random = new Random();
    for (int i = 0; i < seta.length; i++) {
      seta[i] = random.nextInt(0, 4096);
      setb[i] = random.nextInt(0, 4096);
      mask[i] = random.nextBoolean();
    }
  }

  @Benchmark
  public void arraySum(Blackhole bh) {
    int[] result = new int[seta.length];
    for (int i = 0; i < seta.length; i++) {
      result[i] = seta[i] + setb[i];
    }
    //sum(result);
    bh.consume(result);
  }

  @Benchmark
  public void arraySumWithMask(Blackhole bh) {
    int[] result = new int[seta.length];
    for (int i = 0; i < seta.length; i++) {
      if (mask[i])
        result[i] = seta[i] + setb[i];
    }
    //sum(result);
    bh.consume(result);
  }

  @Benchmark
  public void vectorSum(Blackhole bh) {
    int[] result = new int[seta.length];
    VectorMask<Integer> m = SPECIES.indexInRange(0, seta.length);
    for (int i = 0; i < seta.length; i += SPECIES.length()) {
      if (seta.length - i < SPECIES.length()) {
        m = SPECIES.indexInRange(i, seta.length - i);
      }
      IntVector va = IntVector.fromArray(SPECIES, seta, i, m);
      IntVector vb = IntVector.fromArray(SPECIES, setb, i, m);
      va.add(vb).intoArray(result, i);
    }
    //sum(result);
    bh.consume(result);
  }

  @Benchmark
  public void vectorSumWithMask(Blackhole bh) {
    int[] result = new int[seta.length];
    for (int i = 0; i < seta.length; i += SPECIES.length()) {
      VectorMask<Integer> m = SPECIES.loadMask(mask, i);
      IntVector va = IntVector.fromArray(SPECIES, seta, i, m);
      IntVector vb = IntVector.fromArray(SPECIES, setb, i, m);
      va.add(vb).intoArray(result, i);
    }
    //sum(result);
    bh.consume(result);
  }

  private void sum(int[] array) {
    long sum = 0;
    for (int i = 0; i < array.length; i++) {
      sum += array[i];
    }
    System.out.println("Sum: " + sum);
  }

  public static void main(String[] args) {
    VectorIntCrossLaneAggregations v = new VectorIntCrossLaneAggregations();
    v.setup();
    Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    v.arraySum(bh);
    v.arraySumWithMask(bh);
    v.vectorSum(bh);
    v.vectorSumWithMask(bh);
  }
}
