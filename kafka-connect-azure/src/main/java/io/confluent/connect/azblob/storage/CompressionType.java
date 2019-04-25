package io.confluent.connect.azblob.storage;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum CompressionType {

    NONE("none", ""),

    GZIP("gzip", ".gz") {
        @Override
        public OutputStream wrapForOutput(OutputStream out) {
            try {
                return new GZIPOutputStream(out, GZIP_BUFFER_SIZE_BYTES);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public InputStream wrapForInput(InputStream in) {
            try {
                return new GZIPInputStream(in);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void finalize(OutputStream compressionFilter) {
            if (compressionFilter instanceof DeflaterOutputStream) {
                try {
                    ((DeflaterOutputStream) compressionFilter).finish();
                } catch (Exception e) {
                    throw new ConnectException(e);
                }
            } else {
                throw new ConnectException("Expected compressionFilter to be a DeflatorOutputStream, "
                        + "but was passed an instance that does not match that type.");
            }
        }
    };

    private static final int GZIP_BUFFER_SIZE_BYTES = 8 * 1024;

    public final String name;
    public final String extension;

    CompressionType(String name, String extension) {
        this.name = name;
        this.extension = extension;
    }

    /**
     * Return the {@link io.confluent.connect.azblob.storage.CompressionType} with given {@code name}.
     *
     * @param name a lowercase compression type name
     * @return a {@link io.confluent.connect.azblob.storage.CompressionType} with the given name
     */
    public static io.confluent.connect.azblob.storage.CompressionType forName(String name) {
        if (NONE.name.equals(name)) {
            return NONE;
        } else if (GZIP.name.equals(name)) {
            return GZIP;
        } else {
            throw new IllegalArgumentException("Unknown compression name: " + name);
        }
    }

    /**
     * Wrap {@code out} with a filter that will compress data with this CompressionType.
     *
     * @param out the {@link OutputStream} to wrap
     * @return a wrapped version of {@code out} that will apply compression
     */
    public OutputStream wrapForOutput(OutputStream out) {
        return out;
    }

    /**
     * Wrap {@code in} with a filter that will decompress data with this CompressionType.
     *
     * @param in the {@link InputStream} to wrap
     * @return a wrapped version of {@code in} that will apply decompression
     */
    public InputStream wrapForInput(InputStream in) {
        return in;
    }

    /**
     * Take any action necessary to finalize filter before the underlying
     * S3OutputStream is committed.
     *
     * <p>Implementations of this method should make sure to handle the case
     * where {@code compressionFilter} is null.
     *
     * @param compressionFilter a wrapped {@link OutputStream}
     */
    public void finalize(OutputStream compressionFilter) {}

}
