import { MediaPlannerService } from '../src/source/media-planner.service';
import { ResolvedSource } from '../src/source/source.types';

describe('MediaPlannerService', () => {
  const planner = new MediaPlannerService();

  const source = (streams: ResolvedSource['streams']): ResolvedSource => ({
    platform: 'generic',
    vid: 'test',
    canonicalUrl: 'http://example.com/video',
    streams,
  });

  it('keeps both when both-capable', () => {
    const plan = planner.plan('both', source({ muxedBoth: { url: 'http://example.com/a.mp4' } }));
    expect(plan.effective).toBe('both');
  });

  it('falls back video->both when single-video unsupported', () => {
    const plan = planner.plan('video', source({ audioOnly: { url: 'http://example.com/a.m4a' } }));
    expect(plan.effective).toBe('both');
    expect(plan.fallbackReason).toBeTruthy();
  });

  it('uses audio when requested and dedicated stream exists', () => {
    const plan = planner.plan('audio', source({ audioOnly: { url: 'http://example.com/a.m4a' } }));
    expect(plan.effective).toBe('audio');
  });

  it('falls back both->audio when source has no video', () => {
    const plan = planner.plan('both', source({ audioOnly: { url: 'http://example.com/a.m4a' } }));
    expect(plan.effective).toBe('audio');
    expect(plan.fallbackReason).toBeTruthy();
  });
});
