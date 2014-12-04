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

import com.google.common.collect.ImmutableMap;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ssh.JschClient.JschChannel;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Vector;

/**
 *
 */
public class SshBlobContainer extends AbstractBlobContainer {

    protected final SshBlobStore blobStore;

    public SshBlobContainer(SshBlobStore blobStore, BlobPath blobPath) throws IOException, JSchException, SftpException {
        super(blobPath);
        this.blobStore = blobStore;

        try (JschChannel channel = blobStore.getClient().getChannel()) {
            channel.mkdirs(blobPath);
        }
    }

    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        try (JschChannel channel = blobStore.getClient().getChannel()) {
            Vector<LsEntry> entries = channel.ls(path());
            if (entries.isEmpty()) {
                return ImmutableMap.of();
            }

            MapBuilder<String, BlobMetaData> builder = MapBuilder.newMapBuilder();
            for (LsEntry entry : entries) {
                if (entry.getAttrs().isReg()) {
                    builder.put(entry.getFilename(), new PlainBlobMetaData(entry.getFilename(), entry.getAttrs().getSize()));
                }
            }
            return builder.immutableMap();
        } catch (SftpException | JSchException e) {
            throw new IOException("Failed to load files in " + path().buildAsString("/"), e);
        }
    }

    public boolean deleteBlob(String blobName) throws IOException {
        BlobPath path = path().add(blobName);
        try (JschChannel channel = blobStore.getClient().getChannel()) {
            channel.rm(path);
            return true;
        } catch (SftpException | JSchException e) {
            return false;
        }
    }

    @Override
    public boolean blobExists(String blobName) {
        BlobPath path = path().add(blobName);
        try (JschChannel channel = blobStore.getClient().getChannel()) {
            Vector<LsEntry> entries = channel.ls(path);
            return !entries.isEmpty();
        } catch (SftpException | JSchException | IOException e) {
            return false;
        }
    }

    @Override
    public InputStream openInput(String blobName) throws IOException {
        BlobPath path = path().add(blobName);
        try {
            JschChannel channel = blobStore.getClient().getChannel();
            return channel.get(path);
        } catch (SftpException | JSchException e) {
            throw new IOException("Failed to load " + path.buildAsString("/"), e);
        }
    }

    @Override
    public OutputStream createOutput(String blobName) throws IOException {
        BlobPath path = path().add(blobName);
        try {
            JschChannel channel = blobStore.getClient().getChannel();
            return channel.put(path);
        } catch (SftpException | JSchException e) {
            throw new IOException("Failed to open " + path.buildAsString("/"), e);
        }
    }
}
