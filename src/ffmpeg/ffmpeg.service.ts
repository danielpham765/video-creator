import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
const ffmpeg = require('fluent-ffmpeg');

@Injectable()
export class FfmpegService {
  constructor(private readonly config: ConfigService) {
    try {
      const ffPath = String(this.config.get('ffmpeg.path') || '').trim();
      if (ffPath) {
        try { ffmpeg.setFfmpegPath(ffPath); } catch (e) { /* ignore if not available */ }
      }
    } catch (e) {
      // ignore config errors
    }
  }
  async merge(videoUrl: string, audioUrl: string, outputPath: string, headers?: Record<string, string>): Promise<void> {
    return new Promise((resolve, reject) => {
      const command = ffmpeg()
        .input(videoUrl)
        .input(audioUrl)
        .outputOptions(['-c copy'])
        .on('end', () => resolve())
        .on('error', (err: any) => reject(err))
        .save(outputPath);
    });
  }

  async mergeParts(listFilePath: string, outputPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const command = ffmpeg()
        .input(listFilePath)
        .inputOptions(['-f', 'concat', '-safe', '0'])
        .outputOptions(['-c', 'copy'])
        .on('end', () => resolve())
        .on('error', (err: any) => reject(err))
        .save(outputPath);
    });
  }
}
