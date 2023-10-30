package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Calls out specific tags into dedicated span level columns.
 *
 * Benchmark                                              Mode  Cnt       Score      Error  Units
 * FilterAndAggregateCallout.appMasksSerial               avgt    5    6572.202 ±  870.303  us/op
 * FilterAndAggregateCallout.appMasksSerialParallel       avgt    5    7350.753 ± 2185.300  us/op
 * FilterAndAggregateCallout.appMasksSerialParallelLight  avgt    5    5430.311 ±  797.441  us/op
 * FilterAndAggregateCallout.appNaive                     avgt    5     396.599 ±   11.061  us/op
 */
@State(Scope.Benchmark)
public class FilterAndAggregateCallout {
  static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  Method loadDictionary = null;

  RootAllocator allocator = new RootAllocator(536_870_912);

  ArrowDohicky arrowDohicky;

  ExecutorService heavyPool = Executors.newFixedThreadPool(4);
  ExecutorService lightPool = Executors.newVirtualThreadPerTaskExecutor();

  String file = null;
  String nf_app_value = null;

  @Setup
  public void setUp() throws Exception {
    Map<String, Map<String, String>> config = new ObjectMapper().readValue(
        this.getClass().getClassLoader().getResourceAsStream("config.json"),
        Map.class
    );
    String key = this.getClass().getSimpleName().replaceAll("_jmhType", "");
    file = config.get(key).get("file");
    nf_app_value = config.get(key).get("nf.app");

    loadDictionary = ArrowReader.class.getDeclaredMethod("loadDictionary", ArrowDictionaryBatch.class);
    loadDictionary.setAccessible(true);
    arrowDohicky = new ArrowDohicky();
  }

  @TearDown
  public void tearDown() {
    arrowDohicky.close();
    allocator.close();
    heavyPool.shutdownNow();
  }

  class ArrowDohicky {

    final ArrowFileReader reader;
    final VarCharVector traceIds;
    final UInt8Vector starts;
    final UInt8Vector ends;
    final UInt8Vector durations;
    final UInt8Vector errorCounts;
    final UInt4Vector rootSvc;
    final VarCharVector rootSvcDict;
    final UInt4Vector  rootSpan;
    final VarCharVector rootSpanDict;

    final ListVector spans;
    final StructVector struct;
    final UInt8Vector timestamps;
    final UInt8Vector duration;
    final VarCharVector spanIds;
    final VarCharVector parentIds;
    final UInt1Vector kinds;
    final UInt4Vector names;
    final VarCharVector namesDict;

    final MapVector tags;
    final StructVector ms;
    final UInt4Vector keys;
    final UInt4Vector values;
    final VarCharVector tagDict;

    final UInt4Vector nf_app;

    final VarCharVector nf_appDict;
    final long[] durationArray;
    ArrowDohicky() {
      try {
        FileInputStream fileInputStream = new FileInputStream(file);
        reader = new ArrowFileReader(fileInputStream.getChannel(), allocator, CommonsCompressionFactory.INSTANCE);
        reader.loadRecordBatch(reader.getRecordBlocks().getFirst());

        var root = reader.getVectorSchemaRoot();

        traceIds = (VarCharVector) root.getVector("trace_id");
        starts = (UInt8Vector) root.getVector("start_time_micros");
        ends = (UInt8Vector) root.getVector("end_time_micros");
        durations = (UInt8Vector) root.getVector("duration_micros");
        errorCounts = (UInt8Vector) root.getVector("error_count");
        rootSvc = (UInt4Vector) root.getVector("root_service_name");
        rootSvcDict = (VarCharVector) reader.getDictionaryVectors().get(rootSvc.getField().getDictionary().getId()).getVector();
        rootSpan = (UInt4Vector) root.getVector("root_span_name");
        rootSpanDict = (VarCharVector) reader.getDictionaryVectors().get(rootSpan.getField().getDictionary().getId()).getVector();

        spans = (ListVector) root.getVector("spans");
        struct = (StructVector) spans.getChildrenFromFields().getFirst();
        timestamps = (UInt8Vector) struct.getChild("timestamp");
        duration = (UInt8Vector) struct.getChild("duration");
        spanIds = (VarCharVector) struct.getChild("spanId");
        parentIds = (VarCharVector) struct.getChild("parentSpanId");
        kinds = (UInt1Vector) struct.getChild("kind");
        names = (UInt4Vector) struct.getChild("name");
        namesDict = (VarCharVector) reader.getDictionaryVectors().get(names.getField().getDictionary().getId()).getVector();
        nf_app = (UInt4Vector) struct.getChild("nf_app");
        nf_appDict = (VarCharVector) reader.getDictionaryVectors().get(nf_app.getField().getDictionary().getId()).getVector();

        tags = (MapVector) struct.getChild("attributes");
        ms = (StructVector) tags.getChildrenFromFields().getFirst();
        keys = (UInt4Vector) ms.getChild("key");
        values = (UInt4Vector) ms.getChild("value");
        tagDict = (VarCharVector) reader
            .getDictionaryVectors()
            .get(keys.getField().getDictionary().getId())
            .getVector();

        durationArray = new long[durations.getValueCount()];
        for (int i = 0; i < durations.getValueCount(); i++) {
          durationArray[i] = durations.get(i);
        }

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private void loadTraces() throws Exception {
    long s = System.nanoTime();
    FileInputStream fileInputStream = new FileInputStream(file);
    try (ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator, CommonsCompressionFactory.INSTANCE);) {
      List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
      var readTags = 0;
      //for (int i = 0; i < recordBlocks.size(); i++) {
        reader.loadNextBatch();

//        if (i > 0) { // first blocks are loaded.
//          for (int i = 0; i < reader.getDictionaryIds().size(); i++) {
//            var dictionaryBatch = reader.readDictionary();
//            loadDictionary.invoke(reader, dictionaryBatch);
//          }
//        }

        var root = reader.getVectorSchemaRoot();
        var traceIds = (VarCharVector) root.getVector("trace_id");
        var starts = (UInt8Vector) root.getVector("start_time_micros");
        var ends = (UInt8Vector) root.getVector("end_time_micros");
        var durations = (UInt8Vector) root.getVector("duration_micros");
        var errorCounts = (UInt8Vector) root.getVector("error_count");
        var rootSvc = (UInt4Vector) root.getVector("root_service_name");
        var rootSvcDict = (VarCharVector) reader.getDictionaryVectors().get(rootSvc.getField().getDictionary().getId()).getVector();
        var rootSpan = (UInt4Vector) root.getVector("root_span_name");
        var rootSpanDict = (VarCharVector) reader.getDictionaryVectors().get(rootSpan.getField().getDictionary().getId()).getVector();

        var spans = (ListVector) root.getVector("spans");
        var struct = (StructVector) spans.getChildrenFromFields().getFirst();
        var timestamps = (UInt8Vector) struct.getChild("timestamp");
        var duration = (UInt8Vector) struct.getChild("duration");
        var spanIds = (VarCharVector) struct.getChild("spanId");
        var parentIds = (VarCharVector) struct.getChild("parentSpanId");
        var kinds = (UInt1Vector) struct.getChild("kind");
        var names = (UInt4Vector) struct.getChild("name");
        var namesDict = (VarCharVector) reader.getDictionaryVectors().get(names.getField().getDictionary().getId()).getVector();
        var nf_app = (UInt4Vector) struct.getChild("nf_app");
        var nf_appDict = (VarCharVector) reader.getDictionaryVectors().get(nf_app.getField().getDictionary().getId()).getVector();

        var tags = (MapVector) struct.getChild("attributes");
        var ms = (StructVector) tags.getChildrenFromFields().getFirst();
        var keys = (UInt4Vector) ms.getChild("key");
        var values = (UInt4Vector) ms.getChild("value");
        var tagDict = reader
            .getDictionaryVectors()
            .get(keys.getField().getDictionary().getId())
            .getVector();

        long endMin = Long.MAX_VALUE;
        long endMax = Long.MIN_VALUE;

        var tidx = 0;
        var totalMatched = 0;
        while (tidx < spans.getValueCount()) {
          StringBuilder sb = new StringBuilder();
          boolean matched = true;
          //val builder = TempoTrace.newBuilder()
          if (!traceIds.isNull(tidx)) {
            sb.append(traceIds.getObject(tidx).toString() + " ");
            if (!starts.isNull(tidx))
              starts.get(tidx);
            if (!ends.isNull(tidx)) {
              long e = ends.get(tidx);
              if (e < endMin) endMin = e;
              if (e > endMax) endMax = e;
              sb.append(e + " ");
              if (!(e >= 1693009800000000L && e <= 1693011600000000L)) {
                matched = false;
              }
            } else matched = false;

            if (!durations.isNull(tidx)) {
              var d = durations.get(tidx);
              sb.append(d+" ");
              if (d < 1) {
                matched = false;
              }
            } else matched = false;

            if (!errorCounts.isNull(tidx))
              errorCounts.get(tidx);
            if (!rootSvc.isNull(tidx))
              rootSvcDict.getObject(rootSvc.get(tidx)).toString();
            if (!rootSpan.isNull(tidx))
              rootSpanDict.getObject(rootSpan.get(tidx)).toString();

            if (!spans.isNull(tidx)) {
              var start = spans.getElementStartIndex(tidx);
              var end = spans.getElementEndIndex(tidx);

              if (end - start < 2) {
                matched = false;
              }
              sb.append((end - start) + "\n");
              boolean hasApp = false;
              for (int spanIdx = start; spanIdx < end; spanIdx++) {
                if (!timestamps.isNull(spanIdx))
                  timestamps.get(spanIdx);
                if (!duration.isNull(spanIdx))
                  duration.get(spanIdx);
                if (!spanIds.isNull(spanIdx))
                  spanIds.getObject(spanIdx).toString();
                if (!parentIds.isNull(spanIdx))
                  parentIds.getObject(spanIdx).toString();
                //if (!kinds.isNull(spanIdx)) spanBuilder.setKind(TempoSpan.KIND.values(kinds.get(spanIdx)))
                if (!names.isNull(spanIdx))
                  namesDict.getObject(names.get(spanIdx)).toString();

                if (!nf_app.isNull(spanIdx)) {
                  var app = nf_appDict.getObject(nf_app.get(spanIdx)).toString();
                  if (app.equals(nf_app_value)) {
                    hasApp = true;

                  }
                }

                if (!tags.isNull(spanIdx)) {
                  var tagsStart = tags.getElementStartIndex(spanIdx);
                  var tagsEnd = tags.getElementEndIndex(spanIdx);

                  var t = tagsStart;
                  sb.append("{");
                  while (t < tagsEnd) {
                    if (t != tagsStart) sb.append(", ");
                    var k = tagDict.getObject(keys.get(t)).toString();
                    sb.append(k + "=");
                    var v = tagDict.getObject(values.get(t)).toString();
                    sb.append(v);
                    if (k.equals("nf.app") && v.equals(nf_app_value)) {
                      hasApp = true;
                    }
                    readTags++;
                    t++;
                  }
                  sb.append("\n");
                }
              }
              if (!hasApp) matched = false;
            }
            tidx++;
            if (matched) {
              //System.out.println(sb);
              totalMatched++;
            }

            //bh.consume(builder.build())
            //ctr += 1
          } else
            tidx = spans.getValueCount();
        }
      //}
      System.out.println("Total: " + totalMatched + "  End range = " + (endMax - endMin) + " Min: " + endMin + " Max: " + endMax);
      System.out.println("TAGS: " + readTags);
    }
    System.out.println("Read in: " + (System.nanoTime() - s) / 1_000_000. + "ms");
  }

  @Benchmark
  public void appNaive(Blackhole bh) {
    var totalMatched = 0;

    long sum = 0;
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    outer:
    for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
      if (!arrowDohicky.ends.isNull(tidx)) {
        long e = arrowDohicky.ends.get(tidx);
        if (!(e >= 1693009800000000L && e <= 1693011600000000L)) continue;
      } else continue;

      long duration = 0;
      if (!arrowDohicky.durations.isNull(tidx)) {
        duration = arrowDohicky.durations.get(tidx);
        if (duration < 1) continue;
      } else continue;

      if (!arrowDohicky.spans.isNull(tidx)) {
        var start = arrowDohicky.spans.getElementStartIndex(tidx);
        var end = arrowDohicky.spans.getElementEndIndex(tidx);

        if (end - start < 2) continue outer;

        for (int spanIdx = start; spanIdx < end; spanIdx++) {
          if (!arrowDohicky.nf_app.isNull(spanIdx)) {
            var app = arrowDohicky.nf_appDict.getObject(arrowDohicky.nf_app.get(spanIdx)).toString();
            if (app.equals(nf_app_value)) {
              if (duration > max) max = duration;
              if (duration < min) min = duration;
              sum += duration;
              continue outer;
            }
          }
        }
      }
    }

    System.out.println("Total: " + totalMatched + " Sum: " + sum + " Min: " + min + " Max: " + max);
    bh.consume(totalMatched);
    bh.consume(sum);
    bh.consume(min);
    bh.consume(max);
  }

  @Benchmark
  public void appMasksSerial(Blackhole bh) {
    var totalMatched = 0;

    int padded = arrowDohicky.spans.getValueCount();
    boolean[] endTimesMask = new boolean[padded];
    for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
      if (!arrowDohicky.ends.isNull(tidx)) {
        long e = arrowDohicky.ends.get(tidx);
        if (e >= 1693009800000000L && e <= 1693011600000000L) endTimesMask[tidx] = true;
      }
    }

    boolean[] durationMask = new boolean[padded];
    for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
      if (!arrowDohicky.durations.isNull(tidx)) {
        var duration = arrowDohicky.durations.get(tidx);
        if (duration >= 1) durationMask[tidx] = true;
      }
    }

    boolean[] appMask = new boolean[padded];
    outer:
    for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
      if (!arrowDohicky.spans.isNull(tidx)) {
        var start = arrowDohicky.spans.getElementStartIndex(tidx);
        var end = arrowDohicky.spans.getElementEndIndex(tidx);

        if (end - start < 2) continue outer;

        for (int spanIdx = start; spanIdx < end; spanIdx++) {
          if (!arrowDohicky.nf_app.isNull(spanIdx)) {
            var app = arrowDohicky.nf_appDict.getObject(arrowDohicky.nf_app.get(spanIdx)).toString();
            if (app.equals(nf_app_value)) {
              appMask[tidx] = true;
              continue outer;
            }
          }
        }
      }
    }

    long sum = 0;
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    int vl = SPECIES.length();
    for (int i = 0; i < arrowDohicky.durationArray.length - vl; i += vl) {
      VectorMask<Long> mask = VectorMask.fromArray(SPECIES, endTimesMask, i);
      mask = mask.and(VectorMask.fromArray(SPECIES, durationMask, i));
      mask = mask.and(VectorMask.fromArray(SPECIES, appMask, i));

      LongVector v = LongVector.fromArray(SPECIES, arrowDohicky.durationArray, i, mask);
      sum += v.reduceLanesToLong(VectorOperators.ADD, mask);
      long mx = v.reduceLanesToLong(VectorOperators.MAX, mask);
      if (mx > max) max = mx;
      long mn = v.reduceLanesToLong(VectorOperators.MIN, mask);
      if (mn < min) min = mn;
    }

    System.out.println("Total: " + "NA" + " Sum: " + sum + " Min: " + min + " Max: " + max);
    bh.consume(totalMatched);
    bh.consume(sum);
    bh.consume(min);
    bh.consume(max);
  }

  @Benchmark
  public void appMasksSerialParallel(Blackhole bh) throws Exception {
    threadit(bh, heavyPool);
  }

  @Benchmark
  public void appMasksSerialParallelLight(Blackhole bh) throws Exception {
    threadit(bh, lightPool);
  }

  private void threadit(Blackhole bh, ExecutorService ex) throws Exception {
    List<CompletableFuture<boolean[]>> futures = new ArrayList<>();
    int padded = arrowDohicky.spans.getValueCount();

    futures.add(CompletableFuture.supplyAsync(() -> {
      boolean[] endTimesMask = new boolean[padded];
      for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
        if (!arrowDohicky.ends.isNull(tidx)) {
          long e = arrowDohicky.ends.get(tidx);
          if (e >= 1693009800000000L && e <= 1693011600000000L) endTimesMask[tidx] = true;
        }
      }
      return endTimesMask;
    }, ex));

    futures.add(CompletableFuture.supplyAsync(() -> {
      boolean[] durationMask = new boolean[padded];
      for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
        if (!arrowDohicky.durations.isNull(tidx)) {
          var duration = arrowDohicky.durations.get(tidx);
          if (duration >= 1) durationMask[tidx] = true;
        }
      }
      return durationMask;
    }, ex));


    futures.add(CompletableFuture.supplyAsync(() -> {
      boolean[] appMask = new boolean[padded];
      outer:
      for (int tidx = 0; tidx < arrowDohicky.spans.getValueCount(); tidx++) {
        if (!arrowDohicky.spans.isNull(tidx)) {
          var start = arrowDohicky.spans.getElementStartIndex(tidx);
          var end = arrowDohicky.spans.getElementEndIndex(tidx);

          if (end - start < 2) continue outer;

          for (int spanIdx = start; spanIdx < end; spanIdx++) {
            if (!arrowDohicky.nf_app.isNull(spanIdx)) {
              var app = arrowDohicky.nf_appDict.getObject(arrowDohicky.nf_app.get(spanIdx)).toString();
              if (app.equals(nf_app_value)) {
                appMask[tidx] = true;
                continue outer;
              }
            }
          }
        }
      }
      return appMask;
    }, ex));

    CompletableFuture<boolean[]>[] arr = new CompletableFuture[3];
    futures.toArray(arr);
    CompletableFuture.allOf(arr).join();

    long sum = 0;
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    int vl = SPECIES.length();
    for (int i = 0; i < arrowDohicky.durationArray.length - vl; i += vl) {
      VectorMask<Long> mask = VectorMask.fromArray(SPECIES, arr[0].get(), i);
      mask = mask.and(VectorMask.fromArray(SPECIES, arr[1].get(), i));
      mask = mask.and(VectorMask.fromArray(SPECIES, arr[2].get(), i));

      LongVector v = LongVector.fromArray(SPECIES, arrowDohicky.durationArray, i, mask);
      sum += v.reduceLanesToLong(VectorOperators.ADD, mask);
      long mx = v.reduceLanesToLong(VectorOperators.MAX, mask);
      if (mx > max) max = mx;
      long mn = v.reduceLanesToLong(VectorOperators.MIN, mask);
      if (mn < min) min = mn;
    }

    System.out.println("Total: " + "NA" + " Sum: " + sum + " Min: " + min + " Max: " + max);
    bh.consume(sum);
    bh.consume(min);
    bh.consume(max);
  }

  public static void main(String[] args) throws Exception {
    FilterAndAggregateCallout v = new FilterAndAggregateCallout();
    v.setUp();
    v.loadTraces();
    Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    v.appNaive(bh);
    v.appMasksSerial(bh);
    v.appMasksSerialParallel(bh);
    v.tearDown();
  }

}
