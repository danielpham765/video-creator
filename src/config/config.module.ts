import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import loadYamlConfig from './config.loader';
import { RuntimeConfigService } from './runtime-config.service';
import { RuntimeConfigController } from './runtime-config.controller';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [loadYamlConfig],
      // allow env vars to override YAML values
      ignoreEnvFile: true,
    }),
  ],
  providers: [RuntimeConfigService],
  controllers: [RuntimeConfigController],
  exports: [RuntimeConfigService],
})
export class AppConfigModule {}
