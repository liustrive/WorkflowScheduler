/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.wfapp;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * IO Utility methods.
 */
public abstract class IOUtils {

    /**
     * Delete recursively a local directory.
     *
     * @param file directory to delete.
     * @throws IOException thrown if the directory could not be deleted.
     */
    public static void delete(File file) throws IOException {
        ParamChecker.notNull(file, "file");
        if (file.getAbsolutePath().length() < 5) {
            throw new RuntimeException("Path[{0}] is too short, not deleting"+ file.getAbsolutePath());
        }
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] children = file.listFiles();
                if (children != null) {
                    for (File child : children) {
                        delete(child);
                    }
                }
            }
            if (!file.delete()) {
                throw new RuntimeException("Could not delete path[{0}]"+ file.getAbsolutePath());
            }
        }
    }

    /**
     * Return a reader as string. <p/>
     *
     * @param reader reader to read into a string.
     * @param maxLen max content length allowed, if -1 there is no limit.
     * @return the reader content.
     * @throws IOException thrown if the resource could not be read.
     */
    public static String getReaderAsString(Reader reader, int maxLen) throws IOException {
        ParamChecker.notNull(reader, "reader");
        StringBuffer sb = new StringBuffer();
        char[] buffer = new char[2048];
        int read;
        int count = 0;
        while ((read = reader.read(buffer)) > -1) {
            count += read;
            if (maxLen > -1 && count > maxLen) {
                throw new IllegalArgumentException("stream exceeds limit [{0}]"+ maxLen);
            }
            sb.append(buffer, 0, read);
        }
        reader.close();
        return sb.toString();
    }


    /**
     * Return a classpath resource as a stream. <p/>
     *
     * @param path classpath for the resource.
     * @param maxLen max content length allowed.
     * @return the stream for the resource.
     * @throws IOException thrown if the resource could not be read.
     */
    public static InputStream getResourceAsStream(String path, int maxLen) throws IOException {
        ParamChecker.notEmpty(path, "path");
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException("resource [{0}] not found"+path);
        }
        return is;
    }

    /**
     * Return a classpath resource as a reader. <p/> It is assumed that the resource is a text resource.
     *
     * @param path classpath for the resource.
     * @param maxLen max content length allowed.
     * @return the reader for the resource.
     * @throws IOException thrown if the resource could not be read.
     */
    public static Reader getResourceAsReader(String path, int maxLen) throws IOException {
        return new InputStreamReader(getResourceAsStream(path, maxLen));
    }

    /**
     * Return a classpath resource as string. <p/> It is assumed that the resource is a text resource.
     *
     * @param path classpath for the resource.
     * @param maxLen max content length allowed.
     * @return the resource content.
     * @throws IOException thrown if the resource could not be read.
     */
    public static String getResourceAsString(String path, int maxLen) throws IOException {
        ParamChecker.notEmpty(path, "path");
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException("resource [{0}] not found"+ path);
        }
        Reader reader = new InputStreamReader(is);
        return getReaderAsString(reader, maxLen);
    }

    /**
     * Copies an inputstream into an output stream.
     *
     * @param is inputstream to copy from.
     * @param os outputstream to  copy to.
     * @throws IOException thrown if the copy failed.
     */
    public static void copyStream(InputStream is, OutputStream os) throws IOException {
        ParamChecker.notNull(is, "is");
        ParamChecker.notNull(os, "os");
        byte[] buffer = new byte[4096];
        int read;
        while ((read = is.read(buffer)) > -1) {
            os.write(buffer, 0, read);
        }
        os.close();
        is.close();
    }

    /**
     * Copies an char input stream into an char output stream.
     *
     * @param reader reader to copy from.
     * @param writer writer to  copy to.
     * @throws IOException thrown if the copy failed.
     */
    public static void copyCharStream(Reader reader, Writer writer) throws IOException {
        ParamChecker.notNull(reader, "reader");
        ParamChecker.notNull(writer, "writer");
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
        writer.close();
        reader.close();
    }

}


