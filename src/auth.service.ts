import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { GraphQLClient } from 'graphql-request';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);
  private client: GraphQLClient;
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;

  constructor(private configService: ConfigService) {
    const endpoint = this.configService.getOrThrow<string>('GRAPHQL_ENDPOINT');
    this.client = new GraphQLClient(endpoint);
  }

  async getAuthToken(): Promise<string> {
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return this.accessToken;
    }

    await this.authenticate();
    return this.accessToken!;
  }

  private async authenticate(): Promise<void> {
    const email = this.configService.get<string>('ADMIN_EMAIL');
    const password = this.configService.get<string>('ADMIN_PASSWORD');

    const mutation = `
      mutation SignInUser {
        signInUser(signInCredentials: { email: "${email}", password: "${password}" }) {
          accessToken
        }
      }
    `;

    try {
      const response: any = await this.client.request(mutation);
      this.accessToken = response.signInUser.accessToken;
      this.tokenExpiry = new Date(Date.now() + 86400000); // 24 hours

      this.client.setHeader('Authorization', `Bearer ${this.accessToken}`);
      this.logger.log('Authentication successful');
    } catch (error) {
      this.logger.error('Authentication failed:', error);
      throw new Error('Authentication failed');
    }
  }

  getClient(): GraphQLClient {
    return this.client;
  }
}
