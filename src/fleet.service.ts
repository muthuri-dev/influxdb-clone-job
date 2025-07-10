import { Injectable, Logger } from '@nestjs/common';
import { AuthService } from './auth.service';

@Injectable()
export class FleetService {
  private readonly logger = new Logger(FleetService.name);

  constructor(private authService: AuthService) {}

  async getDistributorId(fleetId: string): Promise<string | null> {
    try {
      await this.authService.getAuthToken();
      const client = this.authService.getClient();

      const query = `
        query GetSpecificItemFleet {
          getSpecificItemFleet(id: "${fleetId}") {
            distributor {
              _id
            }
          }
        }
      `;

      const response: any = await client.request(query);
      const distributorId = response.getSpecificItemFleet?.distributor?._id;

      this.logger.log(`Fleet ${fleetId} -> Distributor ${distributorId}`);
      return distributorId || null;
    } catch (error) {
      this.logger.error(`Failed to get distributor for fleet ${fleetId}`);
      return null;
    }
  }
}
