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

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.ssh.JschClient.JschChannel;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * SSH(SFTP) based BlobStore implementation.
 *
 * @author shinsuke
 */
public class SshBlobStore extends AbstractComponent implements BlobStore {

    private JschClient jschClient;

    public SshBlobStore(Settings settings, JschClient jschClient) {
        super(settings);
        this.jschClient = jschClient;
    }

    @Override
    public String toString() {
        return jschClient.getInfoString();
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        try {
            return new SshBlobContainer(this, blobPath);
        } catch (SftpException | JSchException | IOException e) {
            throw new BlobStoreException("Failed to create BlobContainer: " + blobPath, e);
        }
    }

    @Override
    public void delete(BlobPath blobPath) {
        try (JschChannel channel = jschClient.getChannel()) {
            channel.rmdir(blobPath);
        } catch (SftpException | JSchException | IOException e) {
            throw new BlobStoreException("Failed to delete " + blobPath.buildAsString("/"), e);
        }
    }

    @Override
    public void close() {
        jschClient.close();
    }

    public JschClient getClient() {
        return jschClient;
    }

}
