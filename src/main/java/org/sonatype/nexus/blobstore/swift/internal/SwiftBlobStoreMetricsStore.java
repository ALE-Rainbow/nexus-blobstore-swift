/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2017-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.swift.internal;

import com.google.common.collect.ImmutableMap;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Directory;
import org.sonatype.nexus.blobstore.AccumulatingBlobStoreMetrics;
import org.sonatype.nexus.blobstore.BlobStoreMetricsStoreSupport;
import org.sonatype.nexus.blobstore.quota.BlobStoreQuotaService;
import org.sonatype.nexus.common.node.NodeAccess;
import org.sonatype.nexus.scheduling.PeriodicJobService;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A S3 specific {@link BlobStoreMetricsStoreSupport} implementation that retains blobstore metrics in memory,
 * periodically writing them out to S3.
 *
 * @since 3.6.1
 */
@Named
public class SwiftBlobStoreMetricsStore extends BlobStoreMetricsStoreSupport<SwiftPropertiesFile> {

  private static final ImmutableMap<String, Long> AVAILABLE_SPACE_BY_FILE_STORE = ImmutableMap
          .of(SwiftBlobStore.CONFIG_KEY, Long.MAX_VALUE);

  private static final String METRICS_EXTENSION = ".properties";

  private AtomicReference<Directory> directory;
  private String container;

  private Account swift;

  @Inject
  public SwiftBlobStoreMetricsStore(final PeriodicJobService jobService,
                                    final NodeAccess nodeAccess,
                                    final BlobStoreQuotaService quotaService,
                                    @Named("${nexus.blobstore.quota.warnIntervalSeconds:-60}")
                                    final int quotaCheckInterval)
  {
    super(nodeAccess, jobService, quotaService, quotaCheckInterval);
  }

  @Override
  protected SwiftPropertiesFile getProperties() {
    directory = new AtomicReference(new Directory(nodeAccess.getId(), '/'));
    return new SwiftPropertiesFile(swift, container, directory.get(), METRICS_FILENAME);
  }

  @Override
  protected AccumulatingBlobStoreMetrics getAccumulatingBlobStoreMetrics() {
    return new AccumulatingBlobStoreMetrics(0, 0, AVAILABLE_SPACE_BY_FILE_STORE, true);
  }

  public void setContainer(final String container) {
    checkState(this.container == null, "Do not initialize twice");
    checkNotNull(container);
    this.container = container;
  }

  public void setSwift(final Account swift) {
    checkState(this.swift == null, "Do not initialize twice");
    checkNotNull(swift);
    this.swift = swift;
  }

  @Override
  protected Stream<SwiftPropertiesFile> backingFiles() {
    if (swift == null) {
      return Stream.empty();
    } else {
      Stream<SwiftPropertiesFile> stream = swift.getContainer(container).listDirectory(directory.get()).stream()
              .filter(summary -> summary.getName().endsWith(METRICS_EXTENSION))
              .map(summary -> new SwiftPropertiesFile(swift, container, directory.get(), summary.getName()));
      return stream;
    }
  }

  @Override
  public void remove() {
    backingFiles().forEach(metricsFile -> {
      try {
        log.debug("Removing {}", metricsFile);
        metricsFile.remove();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}