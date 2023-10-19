package org.example;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

public class Main {
  static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  public static void main(String[] args) {
    Main m = new Main();
    int[] res = m.addTwoScalarArrays(new int[]{1,2,3}, new int[]{4,5,6});
    System.out.println(Arrays.toString(res));
    res = m.addTwoVectorArrays(new int[]{1,2,3}, new int[]{4,5,6});
    System.out.println(Arrays.toString(res));
    System.out.println(m.findAvg(new int[]{1,2,3,4,5,6,7,8,9,10}));
    System.out.println(m.findMax(new int[]{1,2,3,4,5,6,7,8,9,10}));
  }

  public int[] addTwoScalarArrays(int[] arr1, int[] arr2) {
    int[] result = new int[arr1.length];
    for(int i = 0; i< arr1.length; i++) {
      result[i] = arr1[i] + arr2[i];
    }
    return result;
  }

  public int[] addTwoVectorArrays(int[] arr1, int[] arr2) {
    var mask = SPECIES.indexInRange(0, arr1.length);
    var v1 = IntVector.fromArray(SPECIES, arr1, 0, mask);
    mask = SPECIES.indexInRange(0, arr2.length);
    var v2 = IntVector.fromArray(SPECIES, arr2, 0, mask);
    var result = v1.add(v2);
    return result.toArray();
  }

  public double findAvg(int[] arr) {
    double sum = 0;
    for (int i = 0; i< arr.length; i += SPECIES.length()) {
      var mask = SPECIES.indexInRange(i, arr.length);
      var V = IntVector.fromArray(SPECIES, arr, i, mask);
      sum += V.reduceLanes(VectorOperators.ADD, mask);
    }
    return sum / arr.length;
  }

  public int findMax(int[] arr) {
    int max = -1;
    for (int i = 0; i< arr.length; i += SPECIES.length()) {
      var mask = SPECIES.indexInRange(i, arr.length);
      var V = IntVector.fromArray(SPECIES, arr, i, mask);
      var m = V.reduceLanes(VectorOperators.MAX, mask);
      if (m > max) max = m;
    }
    return max;
  }
}