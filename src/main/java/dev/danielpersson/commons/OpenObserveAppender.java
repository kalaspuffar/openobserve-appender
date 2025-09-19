package dev.danielpersson.commons;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.MDC;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.json.simple.JSONObject;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This appender is used with Log4J to send events to OpenObserve, it uses a memory cache in order to
 * reduce the network traffic balanced with sending enough to get a somewhat realtime view of the system
 * health.
 */
public class OpenObserveAppender extends AppenderSkeleton {
    private String url;
    private String basicUser;
    private String basicPass;
    private String instanceName = "not in cluster";
    private int maxBuffer = 100;
    private int flushIntervalSeconds = 2;
    private int connectTimeoutMillis = 2000;
    private int readTimeoutMillis = 3000;
    private final List<String> buffer = new ArrayList<>();
    private final Object lock = new Object();
    private Timer timer;
    private String host;
    private long pid;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Upper limit of messages to keep in buffer before sending.
     *
     * @param maxBuffer     Max amount of events to store before sending.
     */
    public void setMaxBuffer(int maxBuffer) {
        this.maxBuffer = maxBuffer;
    }

    /**
     * Number of seconds to wait to send events to OpenObserve. Either time or max size will push events.
     *
     * @param flushIntervalSeconds      Number of seconds to keep events in buffer before sending.
     */
    public void setFlushIntervalSeconds(int flushIntervalSeconds) {
        this.flushIntervalSeconds = flushIntervalSeconds;
    }

    /**
     * Connection timeout in milliseconds.
     *
     * @param connectTimeoutMillis     Milliseconds for timeout during connection.
     */
    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /**
     * Read timeout while waiting for a response from the server.
     *
     * @param readTimeoutMillis     Milliseconds for timeout during read of response.
     */
    public void setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    /**
     * Setting the URL to the OpenObserve server you want to send data to, could be behind a load balancer to
     * distribute the load.
     *
     * @param url   URL for OpenObserve server.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Username to the service account of OpenObserve.
     * OpenObserve currently only support basic auth for authentication.
     *
     * @param basicUser     Username to the service account of OpenObserve.
     */
    public void setBasicUser(String basicUser) {
        this.basicUser = basicUser;
    }

    /**
     * Password to the service account of OpenObserve.
     * OpenObserve currently only support basic auth for authentication.
     *
     * @param basicPass    Password to the service account of OpenObserve.
     */
    public void setBasicPass(String basicPass) {
        this.basicPass = basicPass;
    }

    /**
     * This is some extra information you can attach to every event in order to identify them. Mostly used
     * for when you have a cluster of workers and you want to identify an instance that might have problems.
     *
     * @param instanceName      Name of the current java instance running this logger.
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * This function is one of the standard functions from the Appender API that initialize the appender.
     * Preparing some of the variables and initializes the shutdown hook to flush the last events we haven't
     * sent yet.
     */
    @Override
    public void activateOptions() {
        super.activateOptions();
        try {
            this.host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            this.host = "unknown-host";
        }
        this.pid = detectPid();

        timer = new Timer("OpenObserveAppenderFlush", true);
        timer.scheduleAtFixedRate(
            new TimerTask() {
                @Override public void run() { flush(false); }
            },
            flushIntervalSeconds * 1000L,
            flushIntervalSeconds * 1000L
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(
                () -> {
                    try {
                        flush(true);
                    } catch (Throwable ignore) {}
                },
                "OpenObserveAppenderShutdown"
            )
        );
    }

    /**
     * Default append function to add more log rows, we convert the event to json and add them to the queue to be sent.
     *
     * @param event     Log event to send to OpenObserve.
     */
    @Override
    protected void append(LoggingEvent event) {
        try {
            String rec = toJsonRecord(event);
            boolean doFlush = false;
            synchronized (lock) {
                buffer.add(rec);
                if (buffer.size() >= maxBuffer) {
                    doFlush = true;
                }
            }
            if (doFlush) {
                flush(false);
            }
        } catch (Exception e) {
            LogLog.warn("OpenObserveAppender append failed: " + e.getMessage(), e);
        }
    }

    /**
     * When object closes flush the last events not sent yet.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (timer != null) {
                try { timer.cancel(); } catch (Throwable ignore) {}
            }
            flush(true);
        }
    }

    /**
     * As we send all events as JSON there is no need for layout.
     *
     * @return      Always returns false.
     */
    @Override
    public boolean requiresLayout() {
        return false;
    }

    private void flush(boolean finalFlush) {
        List<String> batch;
        synchronized (lock) {
            if (buffer.isEmpty()) return;
            batch = new ArrayList<>(buffer);
            buffer.clear();
        }
        try {
            postBatch(batch);
        } catch (Exception ex) {
            LogLog.warn("OpenObserveAppender POST failed: " + ex.getMessage(), ex);
            if (!finalFlush) {
                synchronized (lock) { buffer.addAll(0, batch); }
            }
        }
    }

    private void postBatch(List<String> batch) throws Exception {
        if (url == null || url.isEmpty()) {
            throw new IllegalStateException("OpenObserveAppender url is not set");
        }
        String payload = "[" + String.join(",", batch) + "]";
        byte[] body = payload.getBytes(StandardCharsets.UTF_8);

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(connectTimeoutMillis);
        conn.setReadTimeout(readTimeoutMillis);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        if (basicUser != null && basicPass != null) {
            String token = Base64.getEncoder()
                .encodeToString((basicUser + ":" + basicPass)
                .getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + token);
        }

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body);
        }

        int code = conn.getResponseCode();
        if (code >= 400) {
            String msg;
            try {
                java.io.InputStream es = conn.getErrorStream();
                msg = es != null ? new String(es.readAllBytes(), StandardCharsets.UTF_8) : "";
            } catch (Throwable t) {
                msg = "";
            }
            throw new RuntimeException("HTTP " + code + " from OpenObserve: " + msg);
        }
    }

    private String toJsonRecord(LoggingEvent e) {
        long micros = e.getTimeStamp() * 1000L;

        JSONObject obj = new JSONObject();
        obj.put("_timestamp", String.valueOf(micros));
        obj.put("level", e.getLevel().toString());
        obj.put("message", e.getRenderedMessage());
        obj.put("worker_name", instanceName);
        obj.put("host", host);
        obj.put("pid", pid);
        obj.put("thread", e.getThreadName());
        obj.put("logger", e.getLoggerName());

        Map<String, Object> ctx = mdcSnapshot();
        if (!ctx.isEmpty()) {
            JSONObject ctxObj = new JSONObject();
            for (Map.Entry<String, Object> entry : ctx.entrySet()) {
                ctxObj.put(entry.getKey(), entry.getValue());
            }
            obj.put("ctx", ctxObj);
        }

        String ndc = e.getNDC();
        if (ndc != null && !ndc.isEmpty()) obj.put("ndc", ndc);

        if (e.getThrowableInformation() != null && e.getThrowableInformation().getThrowable() != null) {
            obj.put("exception", throwableToString(e.getThrowableInformation().getThrowable()));
        }

        return obj.toJSONString();
    }

    private Map<String, Object> mdcSnapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) MDC.getContext();
            if (m != null) {
                for (Map.Entry<String, Object> en : m.entrySet()) {
                    Object v = en.getValue();
                    out.put(en.getKey(), v == null ? null : String.valueOf(v));
                }
            }
        } catch (Throwable ignored) { }
        return out;
    }

    private static String throwableToString(Throwable t) {
        try (
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw)
        ) {
            t.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e) {
            return t.toString();
        }
    }

    private static long detectPid() {
        try {
            return (long) ProcessHandle.current().pid();
        } catch (Throwable ignore) { }
        try {
            String jvm = ManagementFactory.getRuntimeMXBean().getName();
            int at = jvm.indexOf('@');
            if (at > 0) return Long.parseLong(jvm.substring(0, at));
        } catch (Throwable ignore) { }
        return -1L;
    }
}
