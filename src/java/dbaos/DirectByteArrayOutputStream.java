package dbaos;

import java.io.ByteArrayOutputStream;

/**
 * ----------------------DON'T USE!-------------------------------------
 */
public class DirectByteArrayOutputStream extends ByteArrayOutputStream {

    public DirectByteArrayOutputStream ()      { super(4096); } // 4kb seems reasonable for a default
    public DirectByteArrayOutputStream (int n) { super(n); }

    /**
     * If the internal buffer requires truncating (i.e. there is still room in it)
     * truncates it and returns it (involves copying).
     * Otherwise, it simply returns the internal buffer (involves no copying).
     *
     * Subsequent calls will return the exact same array, unless a write is called in between.
     * @return the internal buffer, or a copy of it (if truncation is necessary)
     */
    public byte[] toByteArray() {

        if (count != buf.length) {      // there are empty slots
            buf = super.toByteArray(); // re-assign the internal array to the truncated version
        }
        return buf;
    }

    /**
     * Returns the internal buffer without no attempts to truncate it,
     * so consumers should NOT be surprised by (right-justified) nulls.
     * @return the internal buffer
     */
    public byte[] theBytes() { return buf; }
}
