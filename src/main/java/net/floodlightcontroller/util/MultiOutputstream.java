package net.floodlightcontroller.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Victor on 2017/5/23.
 */
public class MultiOutputstream extends OutputStream {
    OutputStream outputStream1,outputStream2;
    public MultiOutputstream(OutputStream stream1, OutputStream stream2 ) throws IOException{
        outputStream1 = stream1;
        outputStream2 = stream2;
    }
    @Override
    public void write(int b) throws IOException {
        outputStream1.write(b);
        outputStream2.write(b);
    }
}
