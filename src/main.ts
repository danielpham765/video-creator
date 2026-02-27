import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';
import { LoggingInterceptor } from './common/logging.interceptor';

async function bootstrap() {
  // Support dynamic AppModule.register() when AppModule is a dynamic module
  const bootstrapModule: any = (AppModule as any)?.register ? (AppModule as any).register() : AppModule;
  const app = await NestFactory.create(bootstrapModule);

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
  // eslint-disable-next-line no-console
  console.log(`Application listening on port ${port}`);
}

bootstrap();
