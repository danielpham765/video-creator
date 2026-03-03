import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';
import { LoggingInterceptor } from './common/logging.interceptor';
import { FileLoggerService } from './common/file-logger.service';

async function bootstrap() {
  const isWorker = String(process.env.WORKER || '').toLowerCase() === 'true';
  // Support dynamic AppModule.register() when AppModule is a dynamic module
  const bootstrapModule: any = (AppModule as any)?.register ? (AppModule as any).register() : AppModule;
  const logger = new FileLoggerService();
  if (isWorker) {
    const appContext = await NestFactory.createApplicationContext(bootstrapModule, { logger });
    appContext.useLogger(logger);
    logger.log('Worker application context started (HTTP server disabled)', 'Bootstrap');
    const shutdown = async () => {
      try {
        await appContext.close();
      } finally {
        process.exit(0);
      }
    };
    process.once('SIGINT', shutdown);
    process.once('SIGTERM', shutdown);
    await new Promise<void>(() => undefined);
  }

  const app = await NestFactory.create(bootstrapModule, { logger });
  app.useLogger(logger);

  // Enable runtime validation and auto-transformation for DTOs
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  // Log incoming requests and responses
  app.useGlobalInterceptors(new LoggingInterceptor());

  // Swagger/OpenAPI setup
  const config = new DocumentBuilder().setTitle('Video Creator API').setVersion('1.0').build();
  const document = SwaggerModule.createDocument(app, config);
  // expose Swagger UI at both /api and /docs for convenience
  SwaggerModule.setup('api', app, document);
  SwaggerModule.setup('docs', app, document);

  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`Application listening on port ${port}`, 'Bootstrap');
}

bootstrap();
