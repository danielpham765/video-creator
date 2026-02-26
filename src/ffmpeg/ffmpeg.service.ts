import { Injectable } from '@nestjs/common';
const ffmpeg = require('fluent-ffmpeg');

@Injectable()
export class FfmpegService {
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
}
