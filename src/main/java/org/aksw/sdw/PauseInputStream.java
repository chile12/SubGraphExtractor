package org.aksw.sdw;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Chile on 3/18/2015.
 */
public class PauseInputStream extends InputStream{

    private InputStream stream;
    private boolean paused = false;
    private int pauseTime = 500;
    private long offset = 0;

    public PauseInputStream()
    {}

    public PauseInputStream(InputStream stream)
    {
        this.stream = stream;
    }

    ExecutorService waitExecutor = Executors.newSingleThreadExecutor();
    Future future = waitExecutor.submit(new Runnable()
    {
        @Override
        public void run() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    });

    @Override
    public int read() throws IOException {
        if(paused)
        {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        offset++;
        return stream.read();
    }

    void unPause()
    {
        paused = false;
    }

    void pause()
    {
        paused = true;
    }

    long stopStream() throws IOException {
        stream.close();
        return offset;
    }

    long getOffset()
    {
        return offset;
    }
}
