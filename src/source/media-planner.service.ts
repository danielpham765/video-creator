import { Injectable } from '@nestjs/common';
import { MediaMode, MediaPlan, ResolvedSource } from './source.types';

@Injectable()
export class MediaPlannerService {
  plan(requested: MediaMode, source: ResolvedSource): MediaPlan {
    const hasDedicatedVideo = Boolean(source.streams.videoOnly || source.streams.dashPair);
    const hasDedicatedAudio = Boolean(source.streams.audioOnly || source.streams.dashPair);
    const hasMuxedBoth = Boolean(source.streams.muxedBoth);
    const hasTrueBoth = Boolean(source.streams.dashPair || source.streams.muxedBoth || (source.streams.videoOnly && source.streams.audioOnly));

    if (requested === 'both') {
      if (hasTrueBoth) return { requested, effective: 'both' };
      if (hasDedicatedAudio) {
        return {
          requested,
          effective: 'audio',
          fallbackReason: 'source has no video track; falling back to audio-only output',
        };
      }
      if (hasDedicatedVideo) {
        return {
          requested,
          effective: 'video',
          fallbackReason: 'source has no audio track; falling back to video-only output',
        };
      }
      return { requested, effective: 'both' };
    }

    if (requested === 'video') {
      if (hasDedicatedVideo) {
        return { requested, effective: 'video' };
      }
      if (hasMuxedBoth) {
        return {
          requested,
          effective: 'video',
          fallbackReason: 'source has no dedicated video stream; downloading both tracks then stripping audio',
        };
      }
      return {
        requested,
        effective: 'both',
        fallbackReason: 'source does not support reliable video-only download; falling back to both',
      };
    }

    if (hasDedicatedAudio) {
      return { requested, effective: 'audio' };
    }
    if (hasMuxedBoth) {
      return {
        requested,
        effective: 'audio',
        fallbackReason: 'source has no dedicated audio stream; downloading both tracks then extracting audio',
      };
    }
    return {
      requested,
      effective: 'both',
      fallbackReason: 'source does not support reliable audio-only download; falling back to both',
    };
  }
}
