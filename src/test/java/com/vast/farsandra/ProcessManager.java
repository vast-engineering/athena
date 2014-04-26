package com.vast.farsandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Launch a process in the foreground get events from the processes output/error streams
 * @author edward
 *
 */
public class ProcessManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessManager.class);

    private final Process process;

    private final StreamReader outReader;
    private final StreamReader errorReader;

    private ProcessManager(Process process, StreamReader outReader, StreamReader errorReader) {
        this.process = process;
        this.outReader = outReader;
        this.errorReader = errorReader;
    }

    public void addOutLineHandler(LineHandler h){
        outReader.addHandler(h);
    }

    public void addErrLineHandler(LineHandler h){
        errorReader.addHandler(h);
    }

    public static ProcessManager launch(String[] commands) {
        return launch(commands, null, 0);
    }

    public static ProcessManager launch(String[] commands, String launchPattern, int timeout) {
        return launch(commands, launchPattern, timeout, Collections.<LineHandler>emptyList(), Collections.<LineHandler>emptyList());
    }

    public static ProcessManager launch(String[] commands, String launchPattern, int timeout, List<LineHandler> outHandlers, List<LineHandler> errorHandlers) {
        try {
            Thread outstreamThread;
            Thread errstreamThread;

            LOGGER.debug("Launching with - {}", Arrays.asList(commands));
            Runtime rt = Runtime.getRuntime();
            Process process = rt.exec(commands);

            InputStream output = process.getInputStream();
            InputStream error = process.getErrorStream();
            StreamReader outReader = new StreamReader(output);
            for (LineHandler h : outHandlers){
                outReader.addHandler(h);
            }
            outstreamThread = new Thread(outReader);
            StreamReader errReader = new StreamReader(error);
            for (LineHandler h: errorHandlers){
                errReader.addHandler(h);
            }
            errstreamThread = new Thread(errReader);
            outstreamThread.start();
            errstreamThread.start();

            if(launchPattern != null) {
                final CountDownLatch started = new CountDownLatch(1);
                outReader.addHandler(new LineHandler() {
                    @Override
                    public void handleLine(String line) {
                        if (line.contains("Listening for thrift clients...")) started.countDown();
                    }
                });
                try {
                    started.await(timeout, TimeUnit.MILLISECONDS);
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ProcessManager(process, outReader, errReader);
        } catch(IOException e) {
            LOGGER.error("Error launching external process. {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * End the process but do not wait for shutdown latch. Non blocking
     */
    public void destroy(){
        process.destroy();
    }

    public int waitForShutdown(int timeout) throws InterruptedException {
        final CountDownLatch waitForShutdown = new CountDownLatch(1);
        final AtomicInteger exitValue = new AtomicInteger();
        Thread waitForTheEnd = new Thread() {
            public void run() {
                try {
                    exitValue.set(process.waitFor());
                    waitForShutdown.countDown();
                } catch (InterruptedException e) {
                    waitForShutdown.countDown();
                }
            }
        };
        waitForTheEnd.start();
        waitForShutdown.await(timeout, TimeUnit.MILLISECONDS);
        return exitValue.get();
    }


    /**
     * Wait a certain number of seconds for a shutdown. Throw up violently if it takes too long
     * @throws InterruptedException
     */
    public int destroyAndWaitForShutdown(int timeout) throws InterruptedException {
        destroy();
        return waitForShutdown(timeout);
    }
}