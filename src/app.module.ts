import { Module } from '@nestjs/common';
import { AuthService } from './auth.service';
import { FleetService } from './fleet.service';
import { InfluxService } from './influx.service';
import { RedistributionService } from './redistribution.service';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), ScheduleModule.forRoot()],
  controllers: [],
  providers: [AuthService, FleetService, InfluxService, RedistributionService],
})
export class AppModule {}
