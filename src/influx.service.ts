import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InfluxDB, Point } from '@influxdata/influxdb-client';
import { BucketsAPI } from '@influxdata/influxdb-client-apis';

@Injectable()
export class InfluxService {
  private readonly logger = new Logger(InfluxService.name);
  private influxDB: InfluxDB;
  private org: string;
  private bucketsAPI: BucketsAPI;
  private lastProcessedTimes = new Map<string, string>();

  // Cache for bucket existence
  private bucketExistsCache = new Map<string, boolean>();
  private lastBucketCacheRefresh = 0;
  private readonly BUCKET_CACHE_TTL = 60 * 1000; // 1 minute

  constructor(private configService: ConfigService) {
    const url = this.configService.getOrThrow<string>('INFLUXDB_URL');
    const token = this.configService.getOrThrow<string>('INFLUXDB_TOKEN');
    this.org = this.configService.getOrThrow<string>('INFLUXDB_ORG_ID');

    this.influxDB = new InfluxDB({ url, token });
    this.bucketsAPI = new BucketsAPI(this.influxDB);
  }

  async getAllBuckets(): Promise<string[]> {
    const allNames: string[] = [];
    const pageSize = 100;
    let offset = 0;

    while (true) {
      const resp = await this.bucketsAPI.getBuckets({
        orgID: this.org,
        limit: pageSize,
        offset,
      });

      const names = resp.buckets?.map((b) => b.name!) || [];
      allNames.push(...names);
      if (names.length < pageSize) break;
      offset += pageSize;
    }

    this.logger.debug(`Found ${allNames.length} buckets`);
    return allNames;
  }

  async getFleetBuckets(): Promise<string[]> {
    const allBuckets = await this.getAllBuckets();
    const fleetBuckets = allBuckets.filter((bucket) =>
      bucket.startsWith('fleet_'),
    );
    this.logger.debug(`Found ${fleetBuckets.length} fleet buckets`);
    return fleetBuckets;
  }

  // Quick check if bucket has recent data (much faster than full query)
  async hasRecentData(
    bucketName: string,
    hours: number = 24,
  ): Promise<boolean> {
    const queryApi = this.influxDB.getQueryApi(this.org);

    const query = `
      from(bucket: "${bucketName}")
        |> range(start: -${hours}h)
        |> filter(fn: (r) => r._measurement == "mqtt_consumer")
        |> limit(n: 1)
        |> count()
    `;

    return new Promise<boolean>((resolve) => {
      let hasData = false;

      queryApi.queryRows(query, {
        next: (row, tableMeta) => {
          const record = tableMeta.toObject(row);
          hasData = record._value > 0;
        },
        error: (err) => {
          this.logger.error(`Quick data check error for ${bucketName}:`, err);
          resolve(false);
        },
        complete: () => {
          resolve(hasData);
        },
      });
    });
  }

  async getRecentData(
    bucketName: string,
    limit: number = 1000,
  ): Promise<any[]> {
    const queryApi = this.influxDB.getQueryApi(this.org);

    const lastProcessed = this.lastProcessedTimes.get(bucketName);
    const startTime = lastProcessed ? `time(v: "${lastProcessed}")` : '-1h'; // Reduced from -1d

    const query = `
      from(bucket: "${bucketName}")
        |> range(start: ${startTime})
        |> filter(fn: (r) => r._measurement == "mqtt_consumer")
        |> sort(columns: ["_time"])
        |> limit(n: ${limit})
    `;

    return new Promise<any[]>((resolve, reject) => {
      const rows: any[] = [];
      let maxTimestamp = '';

      queryApi.queryRows(query, {
        next: (row, tableMeta) => {
          const record = tableMeta.toObject(row);
          rows.push(record);
          if (!maxTimestamp || record._time > maxTimestamp) {
            maxTimestamp = record._time;
          }
        },
        error: (err) => {
          this.logger.error(`Query error for ${bucketName}:`, err);
          reject(err);
        },
        complete: () => {
          if (rows.length > 0) {
            this.lastProcessedTimes.set(bucketName, maxTimestamp);
            this.logger.debug(
              `Found ${rows.length} new records in ${bucketName}`,
            );
          }
          resolve(rows);
        },
      });
    });
  }

  async writeData(bucketName: string, data: any[]): Promise<void> {
    if (data.length === 0) return;

    const writeApi = this.influxDB.getWriteApi(this.org, bucketName);

    // Set write options for better performance
    writeApi.useDefaultTags({});

    try {
      const points = data.map((record) => {
        const p = new Point(record._measurement).timestamp(
          new Date(record._time),
        );

        p.floatField(record._field, record._value);

        if (record.fleet_id) p.tag('fleet_id', record.fleet_id);
        if (record.mqtt_topic) p.tag('mqtt_topic', record.mqtt_topic);
        if (record.oem_id) p.tag('oem_id', record.oem_id);

        return p;
      });

      writeApi.writePoints(points);
      await writeApi.close();

      this.logger.debug(`✓ Wrote ${points.length} records to ${bucketName}`);
    } catch (error) {
      this.logger.error(`Failed to write to ${bucketName}:`, error);
      throw error;
    }
  }

  async createBucket(bucketName: string): Promise<boolean> {
    try {
      // 1) If we’ve already seen this bucket, return cached result
      if (this.bucketExistsCache.has(bucketName)) {
        return this.bucketExistsCache.get(bucketName)!;
      }

      // 2) Refresh our cache if it’s stale
      if (Date.now() - this.lastBucketCacheRefresh > this.BUCKET_CACHE_TTL) {
        await this.refreshBucketCache();
      }

      // 3) If cache now knows it exists, return true
      if (this.bucketExistsCache.has(bucketName)) {
        return this.bucketExistsCache.get(bucketName)!;
      }

      this.logger.log(`Creating bucket: ${bucketName}`);
      // 4) Try to create it
      await this.bucketsAPI.postBuckets({
        body: {
          name: bucketName,
          orgID: this.org,
          retentionRules: [{ type: 'expire', everySeconds: 30 * 24 * 60 * 60 }],
        },
      });

      // 5) Success → cache and return
      this.bucketExistsCache.set(bucketName, true);
      this.logger.log(`✓ Created bucket: ${bucketName}`);
      return true;
    } catch (error: any) {
      // 6) If Influx replies “already exists” (HTTP 422), that’s OK
      if (error.statusCode === 422) {
        this.logger.log(
          `Bucket ${bucketName} already exists; skipping creation.`,
        );
        this.bucketExistsCache.set(bucketName, true);
        return true;
      }

      // 7) All other errors really are errors
      this.logger.error(`Failed to create bucket ${bucketName}:`, error);
      this.bucketExistsCache.set(bucketName, false);
      return false;
    }
  }

  private async refreshBucketCache(): Promise<void> {
    try {
      const buckets = await this.bucketsAPI.getBuckets();
      this.bucketExistsCache.clear();

      buckets.buckets?.forEach((bucket) => {
        if (bucket.name) {
          this.bucketExistsCache.set(bucket.name, true);
        }
      });

      this.lastBucketCacheRefresh = Date.now();
      this.logger.debug(
        `Refreshed bucket cache with ${this.bucketExistsCache.size} buckets`,
      );
    } catch (error) {
      this.logger.error('Failed to refresh bucket cache:', error);
    }
  }

  async verifyBucketExists(bucketName: string): Promise<boolean> {
    if (this.bucketExistsCache.has(bucketName)) {
      return this.bucketExistsCache.get(bucketName)!;
    }

    try {
      const buckets = await this.bucketsAPI.getBuckets();
      const exists =
        buckets.buckets?.some((bucket) => bucket.name === bucketName) ?? false;

      this.bucketExistsCache.set(bucketName, exists);
      return exists;
    } catch (error) {
      this.logger.error(`Failed to verify bucket ${bucketName}:`, error);
      return false;
    }
  }

  async testConnection(): Promise<boolean> {
    try {
      await this.bucketsAPI.getBuckets();
      return true;
    } catch (error) {
      this.logger.error('InfluxDB connection failed:', error);
      return false;
    }
  }

  // Get performance stats
  getStats(): any {
    return {
      processedBuckets: this.lastProcessedTimes.size,
      bucketCache: this.bucketExistsCache.size,
      lastBucketCacheRefresh: new Date(
        this.lastBucketCacheRefresh,
      ).toISOString(),
    };
  }

  // Clear all caches
  clearCaches(): void {
    this.lastProcessedTimes.clear();
    this.bucketExistsCache.clear();
    this.lastBucketCacheRefresh = 0;
    this.logger.log('All caches cleared');
  }
}
