package athena.testutils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class StreamReader implements Runnable {

    private final InputStream is;
    private final List<LineHandler> handlers = new ArrayList<>();

    public StreamReader(InputStream is){
        this.is = is;
    }

    public void addHandler(LineHandler handler){
        handlers.add(handler);
    }

    public void removeHandler(LineHandler handler) {
        handlers.remove(handler);
    }

    @Override
    public void run() {
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))){
            while ((line = br.readLine()) != null){
                for (LineHandler h: handlers){
                    h.handleLine(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}