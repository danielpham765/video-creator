import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import loadYamlConfig from './config.loader';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [loadYamlConfig],
      // allow env vars to override YAML values
      ignoreEnvFile: true,
    }),
  ],
})
export class AppConfigModule {}
