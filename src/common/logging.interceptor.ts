import { Injectable, NestInterceptor, ExecutionContext, CallHandler, Logger } from '@nestjs/common';
import { Observable, throwError } from 'rxjs';
import { tap, catchError } from 'rxjs/operators';

@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  private readonly logger = new Logger('HTTP');

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const ctx = context.switchToHttp();
    const req: any = ctx.getRequest();
    const { method, url, params, query, body } = req || {};

    try {
      this.logger.log(`Request ${method} ${url} params=${JSON.stringify(params || {})} query=${JSON.stringify(query || {})} body=${JSON.stringify(body || {})}`);
    } catch (e) {
      this.logger.log(`Request ${method} ${url} (body/params omitted - stringify failed)`);
    }

    const now = Date.now();

    return next.handle().pipe(
      tap((data) => {
        const elapsed = Date.now() - now;
        try {
          this.logger.log(`Response ${method} ${url} status=200 time=${elapsed}ms body=${JSON.stringify(data)}`);
        } catch (e) {
          this.logger.log(`Response ${method} ${url} status=200 time=${elapsed}ms (body omitted - stringify failed)`);
        }
      }),
      catchError((err) => {
        const elapsed = Date.now() - now;
        this.logger.error(`Response ${method} ${url} error=${String(err?.message || err)} time=${elapsed}ms`);
        return throwError(() => err);
      }),
    );
  }
}
