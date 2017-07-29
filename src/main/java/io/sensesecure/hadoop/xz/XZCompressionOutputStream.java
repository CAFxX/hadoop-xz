package io.sensesecure.hadoop.xz;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

/**
 *
 * @author yongtang
 */
public class XZCompressionOutputStream extends CompressionOutputStream {

    private final int presetLevel;

    private final long blockSize;

    private XZOutputStream xzOut;

    private boolean resetStateNeeded;

    private long blockOffset;

    public XZCompressionOutputStream(OutputStream out, int presetLevel, long blockSize) throws IOException {
        super(out);
        this.presetLevel = presetLevel;
        this.blockSize = blockSize;
        resetState();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            int chunk = (int) (blockOffset + len < blockSize ? len : blockSize - blockOffset);
            xzOut.write(b, off, chunk);
            off += chunk;
            len -= chunk;
            blockOffset += chunk;
            if (blockOffset == blockSize) {
                xzOut.endBlock();
                blockOffset = 0;
            }
        }
    }

    @Override
    public void finish() throws IOException {
        xzOut.finish();
    }

    @Override
    public void resetState() throws IOException {
        xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
        blockOffset = 0;
    }

    @Override
    public void write(int b) throws IOException {
        xzOut.write(b);
        blockOffset++;
        if (blockOffset == blockSize) {
            xzOut.endBlock();
            blockOffset = 0;
        }
    }

    @Override
    public void flush() throws IOException {
        xzOut.flush();
    }

    @Override
    public void close() throws IOException {
        xzOut.flush();
        xzOut.close();
    }
}
