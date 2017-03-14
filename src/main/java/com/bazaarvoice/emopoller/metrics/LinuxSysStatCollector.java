package com.bazaarvoice.emopoller.metrics;

import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Float.parseFloat;

public class LinuxSysStatCollector {
    private static Logger LOG = LoggerFactory.getLogger(LinuxSysStatCollector.class);

    // ====================================
    // memory stats
    // ====================================

    /*
    Example:
    # cat /proc/meminfo
    MemTotal:         503264 kB
    MemFree:            5528 kB
    MemAvailable:     168604 kB
    Buffers:           41068 kB
    Cached:           112276 kB
    SwapCached:            0 kB
    Active:           389484 kB
    Inactive:          61496 kB
    Active(anon):     297656 kB
    Inactive(anon):       52 kB
    Active(file):      91828 kB
    Inactive(file):    61444 kB
    Unevictable:           0 kB
    Mlocked:               0 kB
    SwapTotal:             0 kB
    SwapFree:              0 kB
    Dirty:              2072 kB
    Writeback:             0 kB
    AnonPages:        297676 kB
    Mapped:            18232 kB
    Shmem:                60 kB
    Slab:              31124 kB
    SReclaimable:      20232 kB
    SUnreclaim:        10892 kB
    KernelStack:        4080 kB
    PageTables:         3916 kB
    NFS_Unstable:          0 kB
    Bounce:                0 kB
    WritebackTmp:          0 kB
    CommitLimit:      251632 kB
    Committed_AS:     546416 kB
    VmallocTotal:   34359738367 kB
    VmallocUsed:           0 kB
    VmallocChunk:          0 kB
    AnonHugePages:         0 kB
    HugePages_Total:       0
    HugePages_Free:        0
    HugePages_Rsvd:        0
    HugePages_Surp:        0
    Hugepagesize:       2048 kB
    DirectMap4k:       12288 kB
    DirectMap2M:      512000 kB
     */

    public static final class LinuxMemStat {
        private final long value;
        @Nullable private final String unit;

        LinuxMemStat(final long value, @Nullable final String unit) {
            this.value = value;
            this.unit = unit;
        }

        public long getValue() { return value; }

        @Nullable
        public String getUnit() { return unit; }
    }

    static Map<String, LinuxMemStat> getMemStats() {
        try (BufferedReader b = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line = "";
            final ImmutableMap.Builder<String,LinuxMemStat> result = ImmutableMap.builder();

            while ((line = b.readLine()) != null) {
                final String[] split = line.split("(:|\\s)+");
                final String metric = split[0].trim();
                final long value = Long.valueOf(split[1].trim());
                final String unit;
                if (split.length > 2) {
                    unit = split[2].trim();
                } else {
                    unit = null;
                }
                result.put(metric,new LinuxMemStat(value,unit));
            }
            b.close();

            return result.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    // ====================================
    // system load
    // ====================================

    public static final class LinuxSysLoad {
        private final float load1m;
        private final float load5m;
        private final float load15m;

        LinuxSysLoad(final float load1m,
                     final float load5m,
                     final float load15m) {
            this.load1m = load1m;
            this.load5m = load5m;
            this.load15m = load15m;
        }

        public float getLoad1m() {
            return load1m;
        }

        public float getLoad5m() {
            return load5m;
        }

        public float getLoad15m() {
            return load15m;
        }
    }

    private static final Pattern loadRegex = Pattern.compile(".*load average:\\s(?<one>\\d*(?:\\.\\d*)?),\\s*(?<five>\\d*(?:\\.\\d*)?),\\s*(?<fifteen>\\d*(?:\\.\\d*)?).*");
//    private static final Pattern loadRegex = Pattern.compile(".*");

    static LinuxSysLoad getLoad() {
        final ArrayList<String> uptime = execute("uptime");
        final String string = uptime.get(0);
        return extractLoad(string);
    }

    private static LinuxSysLoad extractLoad(final String string) {
        final Matcher matcher = loadRegex.matcher(string);
        if (matcher.matches()) {
            final float one = parseFloat(matcher.group("one"));
            final float five = parseFloat(matcher.group("five"));
            final float fifteen = parseFloat(matcher.group("fifteen"));
            return new LinuxSysLoad(one, five, fifteen);
        } else {
            throw new RuntimeException("Could not parse system load from [" + string + "]");
        }
    }


    // ====================================
    // CPU stats
    // ====================================

    /* Examples:
    [ec2-user@ip-10-100-57-3 ~]$ mpstat -P ALL
    Linux 4.4.11-23.53.amzn1.x86_64 (ip-10-100-57-3)        11/17/2016      _x86_64_        (1 CPU)

    11:44:56 PM  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle
    11:44:56 PM  all    0.39    0.09    0.08    0.34    0.00    0.01    0.05    0.00   99.04
    11:44:56 PM    0    0.39    0.09    0.08    0.34    0.00    0.01    0.05    0.00   99.04

    10028(0) john@john-mbp:~ λ mpstat -P ALL
    Linux 4.4.0-36-generic (john-mbp)       11/17/2016      _x86_64_        (8 CPU)

    05:39:44 PM  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
    05:39:44 PM  all   13.10    0.04    2.43    0.05    0.00    0.17    0.00    0.00    0.00   84.20
    05:39:44 PM    0    7.27    0.00    1.40    0.13    0.00    0.30    0.00    0.00    0.00   90.89
    05:39:44 PM    1   16.92    0.08    3.09    0.07    0.00    0.06    0.00    0.00    0.00   79.78
    05:39:44 PM    2   16.94    0.06    3.01    0.05    0.00    0.28    0.00    0.00    0.00   79.67
    05:39:44 PM    3   17.08    0.01    5.82    0.03    0.00    0.04    0.00    0.00    0.00   77.02
    05:39:44 PM    4   12.89    0.00    2.08    0.02    0.00    0.28    0.00    0.00    0.00   84.73
    05:39:44 PM    5   12.65    0.18    1.64    0.01    0.00    0.01    0.00    0.00    0.00   85.51
    05:39:44 PM    6   15.66    0.03    1.87    0.02    0.00    0.22    0.00    0.00    0.00   82.20
    05:39:44 PM    7   12.62    0.01    1.76    0.01    0.00    0.01    0.00    0.00    0.00   85.59
     */


    private static class LinuxCPUStat {
        private ImmutableMap<String, Float> all;
        private ImmutableList<ImmutableMap<String, Float>> cpus;

        LinuxCPUStat(final ImmutableMap<String, Float> all,
                     final ImmutableList<ImmutableMap<String, Float>> cpus) {
            this.all = all;
            this.cpus = cpus;
        }

        public ImmutableMap<String, Float> getAll() {
            return all;
        }

        public ImmutableList<ImmutableMap<String, Float>> getCpus() {
            return cpus;
        }
    }

    static LinuxCPUStat getCPUStat() {
        final ArrayList<String> mpstat = execute("mpstat -P ALL");

        final String header_row = mpstat.get(2);
        final String[] header = header_row.split("\\s+");

        final String[] all_row = mpstat.get(3).split("\\s+");
        final ImmutableMap<String, Float> all = parseRow(header, all_row);

        final ImmutableList.Builder<ImmutableMap<String, Float>> cpus_builder = ImmutableList.builder();
        for (int i = 4; i < mpstat.size(); i++) {
            final String[] row = mpstat.get(i).split("\\s+");
            cpus_builder.add(parseRow(header, row));
        }

        return new LinuxCPUStat(all, cpus_builder.build());
    }

    private static ImmutableMap<String, Float> parseRow(final String[] header, final String[] row) {
        final ImmutableMap.Builder<String, Float> row_builder = ImmutableMap.builder();
        for (int j = 3; j < header.length; j++) {
            row_builder.put(header[j], parseFloat(row[j]));
        }
        return row_builder.build();
    }

    public static class LinuxNetStats {
        private LinuxNetStats(final ImmutableMap<String, IfaceStats> stats) {this.stats = stats;}

        public static class IfaceStats {
            private final double rx_bytes_per_sec;
            private final double tx_bytes_per_sec;

            private IfaceStats(final double rx_bytes_per_sec, final double tx_bytes_per_sec) {
                this.rx_bytes_per_sec = rx_bytes_per_sec;
                this.tx_bytes_per_sec = tx_bytes_per_sec;
            }

            public double getRxBytesPerSec() {
                return rx_bytes_per_sec;
            }

            public double getTxBytesPerSec() {
                return tx_bytes_per_sec;
            }
        }

        private ImmutableMap<String, IfaceStats> stats;

        public ImmutableMap<String, IfaceStats> getStats() {
            return stats;
        }
    }

    public static class NetMonitor {
        private NetMonitor() {}

        ;

        private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        private final AtomicReference<ImmutableMap<String, ImmutableMap<String, Long>>> lastReading = new AtomicReference<>(readNetStats());
        private final AtomicReference<ImmutableMap<String, ImmutableMap<String, Double>>> rate = new AtomicReference<>(null);
        private final AtomicBoolean started = new AtomicBoolean(false);

        private final AbstractScheduledService ticker = new AbstractScheduledService() {
            @Override protected void runOneIteration() throws Exception {
                try {
                    final long time = System.currentTimeMillis();
                    final ImmutableMap<String, ImmutableMap<String, Long>> reading = readNetStats();
                    final ImmutableMap<String, ImmutableMap<String, Double>> computedRate = computeRate(time - lastTime.get(), lastReading.get(), reading);
                    lastTime.set(time);
                    lastReading.set(reading);
                    rate.set(computedRate);
                } catch (Exception e) {
                    LOG.error("Exception in net monitor", e);
                }
            }

            @Override protected Scheduler scheduler() {
                return Scheduler.newFixedRateSchedule(500, 1000, TimeUnit.MILLISECONDS);
            }
        };

        public void start() {
            final boolean wasStarted = started.getAndSet(true);
            if (!wasStarted) {
                ticker.startAsync().awaitRunning();
            } else {
                ticker.awaitRunning();
            }
        }

        public void stop() {
            ticker.stopAsync().awaitTerminated();
        }

        public ImmutableMap<String, ImmutableMap<String, Double>> getRates() {
            Preconditions.checkState(ticker.isRunning());
            while (rate.get() == null) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return rate.get();
        }

        public ImmutableMap<String, ImmutableMap<String, Long>> getValues() {
            Preconditions.checkState(ticker.isRunning());
            return lastReading.get();
        }

        public LinuxNetStats getNetStats() {
            final Map<String, LinuxNetStats.IfaceStats> map = new HashMap<>();
            for (Map.Entry<String, ImmutableMap<String, Double>> entry : getRates().entrySet()) {
                map.put(entry.getKey(), new LinuxNetStats.IfaceStats(entry.getValue().get("rx_bytes") / 1000, entry.getValue().get("tx_bytes") / 1000));
            }
            return new LinuxNetStats(ImmutableMap.copyOf(map));
        }

        private static ImmutableMap<String, ImmutableMap<String, Long>> readNetStats() {
            final Map<String, ImmutableMap<String, Long>> ifaceMetricMap = new HashMap<>();
            for (File ifaceDir : new File("/sys/class/net/").listFiles()) {
                final String iface = ifaceDir.getName();
                if (!iface.equals("lo")) {
                    final HashMap<String, Long> metricMap = new HashMap<>();
                    final File stats = new File(ifaceDir, "statistics");
                    for (File statFile : stats.listFiles()) {
                        final String stat = statFile.getName();
                        final JsonNode statValue;
                        try {
                            statValue = JsonUtil.mapper().readTree(statFile);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        metricMap.put(stat, statValue.asLong());
                    }
                    ifaceMetricMap.put(iface, ImmutableMap.copyOf(metricMap));
                }
            }
            return ImmutableMap.copyOf(ifaceMetricMap);
        }

        private static ImmutableMap<String, ImmutableMap<String, Double>> computeRate(final long timeDelta, final ImmutableMap<String, ImmutableMap<String, Long>> reading1, final ImmutableMap<String, ImmutableMap<String, Long>> reading2) {
            final Map<String, ImmutableMap<String, Double>> ifaceMetricDiffMap = new HashMap<>();
            for (String iface : Sets.union(reading1.keySet(), reading2.keySet())) {
                if (reading1.containsKey(iface) && reading2.containsKey(iface)) {
                    final ImmutableMap<String, Long> metrics1 = reading1.get(iface);
                    final ImmutableMap<String, Long> metrics2 = reading2.get(iface);
                    final Map<String, Double> diff = new HashMap<>();
                    for (String metric : Sets.union(metrics1.keySet(), metrics2.keySet())) {
                        if (metrics1.containsKey(metric) && metrics2.containsKey(metric)) {
                            final long valueDelta = metrics2.get(metric) - metrics1.get(metric);
                            final double perSecond = (0.0 + valueDelta) / timeDelta * 1000;
                            diff.put(metric, perSecond);
                        } else if (metrics2.containsKey(metric)) {
                            final Long value = metrics2.get(metric);
                            final double perSecond = (0.0 + value) / timeDelta * 1000;
                            diff.put(metric, perSecond);
                        } /*else if (metrics1.containsKey(metric)) {
                        // do nothing
                    }*/
                    }
                    ifaceMetricDiffMap.put(iface, ImmutableMap.copyOf(diff));
                } else if (reading2.containsKey(iface)) {
                    final Map<String, Double> diff = new HashMap<>();
                    for (Map.Entry<String, Long> metric : reading2.get(iface).entrySet()) {
                        final double perSecond = (0.0 + metric.getValue()) / timeDelta * 1000;
                        diff.put(metric.getKey(), perSecond);
                    }
                    ifaceMetricDiffMap.put(iface, ImmutableMap.copyOf(diff));
                } /*else if (reading1.containsKey(iface)) {
                // do nothing
            }*/
            }
            return ImmutableMap.copyOf(ifaceMetricDiffMap);
        }
    }

    public static final NetMonitor netMonitor = new NetMonitor();

    private static ArrayList<String> execute(final String cmd) {
        final ArrayList<String> result = new ArrayList<>();
        try {

            final Runtime r = Runtime.getRuntime();
            final Process p = r.exec(cmd);
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try (BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line = "";

                while ((line = b.readLine()) != null) {
                    result.add(line);
                }

                b.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
