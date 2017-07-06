/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.elasticsearch;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class CountingHttpServletResponseWrapper extends HttpServletResponseWrapper {
    private OutputStreamCounter outputStream = null;
    private PrintWriter writer = null;

    public CountingHttpServletResponseWrapper(HttpServletResponse response) throws IOException { super(response); }
    long getWritten() { return outputStream != null ? outputStream.getWritten() : 0; }

    @Override public synchronized ServletOutputStream getOutputStream() throws IOException {
        if (writer != null) throw new IllegalStateException("getWriter() already called");
        if (outputStream == null) outputStream = new OutputStreamCounter(super.getOutputStream());
        return outputStream;
    }

    @Override public synchronized PrintWriter getWriter() throws IOException {
        if (writer == null && outputStream != null) throw new IllegalStateException("getOutputStream() already called");
        if (writer == null) {
            outputStream = new OutputStreamCounter(super.getOutputStream());
            writer = new PrintWriter(new OutputStreamWriter(outputStream, getCharacterEncoding()));
        }
        return this.writer;
    }

    @Override public void flushBuffer() throws IOException {
        if (writer != null) writer.flush();
        else if (outputStream != null) outputStream.flush();
        super.flushBuffer();
    }

    static class OutputStreamCounter extends ServletOutputStream {
        private long written = 0;
        private ServletOutputStream inner;
        OutputStreamCounter(ServletOutputStream inner) { this.inner = inner; }
        long getWritten() { return written; }

        @Override public void close() throws IOException { inner.close(); }
        @Override public void flush() throws IOException { inner.flush(); }

        @Override public void write(byte[] b) throws IOException { write(b, 0, b.length); }
        @Override public void write(byte[] b, int off, int len) throws IOException { inner.write(b, off, len); written += len; }
        @Override public void write(int b) throws IOException { inner.write(b); written++; }

        @Override public boolean isReady() { return inner.isReady(); }
        @Override public void setWriteListener(WriteListener writeListener) { inner.setWriteListener(writeListener); }
    }
}
