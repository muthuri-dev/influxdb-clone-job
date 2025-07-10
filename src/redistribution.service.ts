import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { FleetService } from './fleet.service';
import { InfluxService } from './influx.service';

@Injectable()
export class RedistributionService {
  private readonly logger = new Logger(RedistributionService.name);
  private isRunning = false;
  private readonly MAX_CONCURRENT_BUCKETS = 10; // Adjust based on your system
  private readonly BATCH_SIZE = 50; // Process buckets in batches

  // Cache for distributor IDs to avoid repeated API calls
  private distributorCache = new Map<string, string | null>();
  private lastCacheCleanup = Date.now();
  private readonly CACHE_TTL = 5 * 60 * 1000; // 5 minutes

  // Track empty buckets to avoid repeated queries
  private emptyBuckets = new Set<string>();
  private lastEmptyCheck = new Map<string, number>();
  private readonly EMPTY_RECHECK_INTERVAL = 30 * 60 * 1000; // 30 minutes

  constructor(
    private fleetService: FleetService,
    private influxService: InfluxService,
  ) {}

  @Cron('*/30 * * * * *') // Every 30 seconds (reduced from 10)
  async redistribute(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Redistribution already running, skipping...');
      return;
    }

    this.isRunning = true;
    const startTime = Date.now();
    this.logger.log('Starting optimized redistribution...');

    try {
      // Clean caches periodically
      this.cleanupCaches();

      // Test connection first
      const connectionOk = await this.influxService.testConnection();
      if (!connectionOk) {
        this.logger.error(
          'InfluxDB connection failed, skipping redistribution',
        );
        return;
      }

      await this.processFleetBucketsOptimized();

      const duration = Date.now() - startTime;
      this.logger.log(`Redistribution completed in ${duration}ms`);
    } catch (error) {
      this.logger.error('Redistribution failed:', error);
    } finally {
      this.isRunning = false;
    }
  }

  private async processFleetBucketsOptimized(): Promise<void> {
    const fleetBuckets = await this.influxService.getFleetBuckets();

    if (fleetBuckets.length === 0) {
      this.logger.debug('No fleet buckets found');
      return;
    }

    // Filter out recently checked empty buckets
    const bucketsToProcess = fleetBuckets.filter((bucket) => {
      if (this.emptyBuckets.has(bucket)) {
        const lastCheck = this.lastEmptyCheck.get(bucket) || 0;
        const shouldRecheck =
          Date.now() - lastCheck > this.EMPTY_RECHECK_INTERVAL;
        if (!shouldRecheck) {
          this.logger.verbose(
            `Skipping empty bucket ${bucket} (last checked recently)`,
          );
          return false;
        }
      }
      return true;
    });

    this.logger.log(
      `Processing ${bucketsToProcess.length} fleet buckets (${fleetBuckets.length - bucketsToProcess.length} skipped as empty)`,
    );

    // Process buckets in batches with concurrency control
    for (let i = 0; i < bucketsToProcess.length; i += this.BATCH_SIZE) {
      const batch = bucketsToProcess.slice(i, i + this.BATCH_SIZE);
      await this.processBatch(batch);
    }
  }

  private async processBatch(buckets: string[]): Promise<void> {
    const promises = buckets.map((bucket) =>
      this.processFleetBucketOptimized(bucket).catch((error) => {
        this.logger.error(`Failed to process bucket ${bucket}:`, error);
        return null; // Don't fail the entire batch
      }),
    );

    // Process with concurrency limit
    const results = await this.limitConcurrency(
      promises,
      this.MAX_CONCURRENT_BUCKETS,
    );

    const successful = results.filter((r) => r !== null).length;
    this.logger.debug(
      `Batch processed: ${successful}/${buckets.length} successful`,
    );
  }

  private async processFleetBucketOptimized(bucketName: string): Promise<any> {
    const fleetId = bucketName.replace('fleet_', '');

    // Quick data check first - if empty, skip everything else
    const hasData = await this.influxService.hasRecentData(bucketName);
    if (!hasData) {
      this.emptyBuckets.add(bucketName);
      this.lastEmptyCheck.set(bucketName, Date.now());
      this.logger.debug(`No data in ${bucketName}, marking as empty`);
      return null;
    }

    // Remove from empty set if it has data now
    if (this.emptyBuckets.has(bucketName)) {
      this.emptyBuckets.delete(bucketName);
      this.logger.debug(
        `Bucket ${bucketName} now has data, removing from empty set`,
      );
    }

    // Get distributor ID with caching
    const distributorId = await this.getDistributorIdCached(fleetId);
    if (!distributorId) {
      this.logger.warn(`No distributor found for fleet ${fleetId}`);
      return null;
    }

    // Get recent data
    const data = await this.influxService.getRecentData(bucketName);
    if (data.length === 0) {
      this.logger.debug(`No new data in ${bucketName}`);
      return null;
    }

    // Create distributor bucket and write data
    const distributorBucket = `distributor_${distributorId}`;

    const bucketReady = await this.ensureBucketExists(distributorBucket);
    if (!bucketReady) {
      this.logger.error(`Failed to ensure bucket ${distributorBucket} exists`);
      return null;
    }

    try {
      await this.influxService.writeData(distributorBucket, data);
      this.logger.log(
        `✓ ${data.length} records: ${bucketName} -> ${distributorBucket}`,
      );
      return { bucketName, distributorBucket, recordCount: data.length };
    } catch (error) {
      this.logger.error(`Failed to write data to ${distributorBucket}:`, error);
      return null;
    }
  }

  private async getDistributorIdCached(
    fleetId: string,
  ): Promise<string | null> {
    // Check cache first
    if (this.distributorCache.has(fleetId)) {
      return this.distributorCache.get(fleetId) ?? null;
    }

    // Fetch from API
    const distributorId = await this.fleetService.getDistributorId(fleetId);

    // Cache the result (even if null)
    this.distributorCache.set(fleetId, distributorId);

    return distributorId;
  }

  private async ensureBucketExists(bucketName: string): Promise<boolean> {
    // This could also be cached, but bucket creation is less frequent
    const created = await this.influxService.createBucket(bucketName);
    return created;
  }

  private async limitConcurrency<T>(
    promises: Promise<T>[],
    limit: number,
  ): Promise<T[]> {
    const results: T[] = [];

    for (let i = 0; i < promises.length; i += limit) {
      const batch = promises.slice(i, i + limit);
      const batchResults = await Promise.all(batch);
      results.push(...batchResults);
    }

    return results;
  }

  private cleanupCaches(): void {
    const now = Date.now();

    // Clean distributor cache
    if (now - this.lastCacheCleanup > this.CACHE_TTL) {
      this.distributorCache.clear();
      this.lastCacheCleanup = now;
      this.logger.debug('Cleared distributor cache');
    }

    // Clean empty bucket tracking (keep for longer)
    const oldEmptyBuckets = Array.from(this.emptyBuckets).filter((bucket) => {
      const lastCheck = this.lastEmptyCheck.get(bucket) || 0;
      return now - lastCheck > this.EMPTY_RECHECK_INTERVAL * 2;
    });

    oldEmptyBuckets.forEach((bucket) => {
      this.emptyBuckets.delete(bucket);
      this.lastEmptyCheck.delete(bucket);
    });

    if (oldEmptyBuckets.length > 0) {
      this.logger.debug(
        `Cleaned ${oldEmptyBuckets.length} old empty bucket entries`,
      );
    }
  }

  // Get performance statistics
  async getStats(): Promise<any> {
    return {
      cacheSize: this.distributorCache.size,
      emptyBuckets: this.emptyBuckets.size,
      isRunning: this.isRunning,
      config: {
        maxConcurrentBuckets: this.MAX_CONCURRENT_BUCKETS,
        batchSize: this.BATCH_SIZE,
        cacheTtl: this.CACHE_TTL,
        emptyRecheckInterval: this.EMPTY_RECHECK_INTERVAL,
      },
    };
  }

  // Method to force refresh caches
  async refreshCaches(): Promise<void> {
    this.distributorCache.clear();
    this.emptyBuckets.clear();
    this.lastEmptyCheck.clear();
    this.logger.log('All caches cleared');
  }

  // Manual trigger with performance tracking
  async triggerRedistribution(): Promise<any> {
    if (this.isRunning) {
      return { status: 'Already running' };
    }

    const startTime = Date.now();
    try {
      await this.redistribute();
      const duration = Date.now() - startTime;
      return {
        status: 'Completed successfully',
        duration: `${duration}ms`,
        stats: await this.getStats(),
      };
    } catch (error) {
      this.logger.error('Manual redistribution failed:', error);
      return { status: `Failed: ${error.message}` };
    }
  }
}
