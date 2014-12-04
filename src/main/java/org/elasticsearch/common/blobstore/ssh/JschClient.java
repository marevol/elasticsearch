/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.blobstore.ssh;

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * JschClient manages SSH connections on JSch.
 * 
 * @author shinsuke
 */
public class JschClient {

    private JSch jsch;

    private String password;

    private String host;

    private String username;

    private int port;

    private String location;

    private String knownHosts;

    private Queue<JschChannel> channelQueue = new ConcurrentLinkedQueue<>();

    public JschClient(final Settings componentSettings, final RepositorySettings repositorySettings, final ThreadPool threadPool)
            throws JSchException {
        password = repositorySettings.settings().get("password", componentSettings.get("password"));
        knownHosts = repositorySettings.settings().get("known_hosts", componentSettings.get("known_hosts"));
        String privateKey = repositorySettings.settings().get("private_key", componentSettings.get("private_key"));

        if (password == null && privateKey == null) {
            throw new JSchException("A password and private key for SSH are empty.");
        }

        jsch = new JSch();
        if (knownHosts != null) {
            jsch.setKnownHosts(knownHosts);
        }

        if (privateKey != null) {
            jsch.addIdentity(privateKey);
        }

        host = repositorySettings.settings().get("host", componentSettings.get("host"));
        username = repositorySettings.settings().get("username", componentSettings.get("username"));
        port = repositorySettings.settings().getAsInt("port", componentSettings.getAsInt("port", 22));
        location = repositorySettings.settings().get("location", componentSettings.get("location", "~/"));

        final long sessionExpiry = repositorySettings.settings().getAsLong("session_expire",
                componentSettings.getAsLong("session_expire", 60000L));
        final TimeValue cleanInterval = repositorySettings.settings().getAsTime("clean_interval",
                componentSettings.getAsTime("clean_interval", TimeValue.timeValueMinutes(1)));
        threadPool.schedule(cleanInterval, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                try {
                    int size = channelQueue.size();
                    JschChannel channel = channelQueue.poll();
                    while (channel != null && size > 0) {
                        if (System.currentTimeMillis() - channel.lastAccessed < sessionExpiry && channel.isConnected()) {
                            channelQueue.offer(channel);
                        }
                        channel = channelQueue.poll();
                        size--;
                    }
                } finally {
                    threadPool.schedule(cleanInterval, ThreadPool.Names.GENERIC, this);
                }
            }
        });
    }

    public String getInfoString() {
        return username + "@" + host + ":" + location;
    }

    public JschChannel getChannel() throws JSchException {
        JschChannel channel = channelQueue.poll();
        if (channel == null) {
            return new JschChannel();
        }
        channel.reconnect();
        return channel;

    }

    @SuppressWarnings("resource")
    public void close() {
        JschChannel channel = channelQueue.poll();
        while (channel != null) {
            channel.disconnect();
            channel = channelQueue.poll();
        }
    }

    public class JschChannel implements Closeable {
        private Session session;

        private ChannelSftp channel;

        private long lastAccessed;

        public JschChannel() throws JSchException {
            createSession();
            openChannel();
            lastAccessed = System.currentTimeMillis();
        }

        public void reconnect() throws JSchException {
            if (!session.isConnected() || !channel.isConnected()) {
                createSession();
                openChannel();
            }
            lastAccessed = System.currentTimeMillis();
        }

        private void createSession() throws JSchException {
            session = jsch.getSession(username, host, port);
            if (password != null) {
                session.setPassword(password);
            }
            if (knownHosts == null) {
                session.setConfig("StrictHostKeyChecking", "no");
            }
            session.connect();
        }

        private void openChannel() throws JSchException {
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
        }

        public void disconnect() {
            if (channel != null && !channel.isClosed()) {
                channel.disconnect();
            }
            if (!session.isConnected()) {
                session.disconnect();
            }
        }

        public boolean isConnected() {
            return session.isConnected();
        }

        @Override
        public void close() throws IOException {
            if (!channelQueue.contains(this)) {
                channelQueue.offer(this);
            }
        }

        public void mkdirs(BlobPath blobPath) throws SftpException {
            String[] paths = blobPath.toArray();
            if (paths.length == 0) {
                return;
            }

            StringBuilder buf = new StringBuilder();
            buf.append(location);
            for (String p : paths) {
                buf.append('/').append(p);
                String path = buf.toString();
                int retry = 5;
                while (retry > 0) {
                    try {
                        mkdirIfNotExists(path);
                        retry = 0;
                    } catch (SftpException e) {
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e1) {
                            // ignore
                        }
                        if (retry == 0) {
                            throw e;
                        }
                        retry--;
                    }
                }
            }
        }

        private void mkdirIfNotExists(String path) throws SftpException {
            try {
                channel.ls(path);
            } catch (SftpException e) {
                channel.mkdir(path);
            }
        }

        public void rmdir(BlobPath blobPath) throws SftpException {
            channel.rmdir(location + "/" + blobPath.buildAsString("/"));
        }

        public Vector<LsEntry> ls(BlobPath blobPath) throws SftpException {
            @SuppressWarnings("unchecked")
            Vector<LsEntry> entities = channel.ls(location + "/" + blobPath.buildAsString("/"));
            return entities;
        }

        public void rm(BlobPath blobPath) throws SftpException {
            channel.rm(location + "/" + blobPath.buildAsString("/"));
        }

        public InputStream get(BlobPath blobPath) throws SftpException {
            final InputStream is = channel.get(location + "/" + blobPath.buildAsString("/"));
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    return is.read();
                }

                @Override
                public void close() throws IOException {
                    is.close();
                    JschChannel.this.close();
                }

            };
        }

        public OutputStream put(BlobPath blobPath) throws SftpException {
            final OutputStream os = channel.put(location + "/" + blobPath.buildAsString("/"));
            return new OutputStream() {

                @Override
                public void write(int b) throws IOException {
                    os.write(b);
                }

                @Override
                public void close() throws IOException {
                    os.close();
                    JschChannel.this.close();
                }
            };
        }
    }

}
