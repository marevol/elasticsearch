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

package org.elasticsearch.repositories.ssh;

import com.jcraft.jsch.JSchException;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ssh.JschClient;
import org.elasticsearch.common.blobstore.ssh.SshBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p/>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code location}</dt>
 * <dd>Path to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt>
 * <dd>Number of concurrent read/write stream (per repository on each node).
 * Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt>
 * <dd>Large file can be divided into chunks. This parameter specifies the chunk
 * size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt>
 * <dd>If set to true metadata files will be stored compressed. Defaults to
 * false.</dd> </ol>
 */
public class SshRepository extends BlobStoreRepository {

    public final static String TYPE = "ssh";

    private final SshBlobStore blobStore;

    private ByteSizeValue chunkSize;

    private final BlobPath basePath;

    private boolean compress;

    /**
     * Constructs new shared file system repository
     *
     * @param name
     *            repository name
     * @param repositorySettings
     *            repository settings
     * @param indexShardRepository
     *            index shard repository
     * @throws IOException
     */
    @Inject
    public SshRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository,
            ThreadPool threadPool) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);

        try {
            blobStore = new SshBlobStore(componentSettings, new JschClient(componentSettings, repositorySettings, threadPool));
        } catch (JSchException e) {
            throw new RepositoryException(name.name(), "Failed to initialize SSH configuration.", e);
        }

        this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size", componentSettings.getAsBytesSize("chunk_size", null));
        this.compress = repositorySettings.settings().getAsBoolean("compress", componentSettings.getAsBoolean("compress", false));
        this.basePath = BlobPath.cleanPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isCompress() {
        return compress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }
}
