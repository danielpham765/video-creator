export const VIDEO_QUALITY_LABELS = ['2160p', '1440p', '1080p', '720p', '480p', '360p'] as const;
export type VideoQualityLabel = typeof VIDEO_QUALITY_LABELS[number];

const LABEL_TO_QN: Record<VideoQualityLabel, number> = {
  '2160p': 120,
  '1440p': 116,
  '1080p': 80,
  '720p': 64,
  '480p': 32,
  '360p': 16,
};

export interface VideoQualityPolicy {
  preferredLabel: VideoQualityLabel;
  preferredQn: number;
  minAcceptableQn: number;
}

export function normalizeVideoQualityLabel(value: any): VideoQualityLabel | null {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) return null;
  if ((VIDEO_QUALITY_LABELS as readonly string[]).includes(normalized)) {
    return normalized as VideoQualityLabel;
  }
  return null;
}

export function qnFromVideoQualityLabel(label: VideoQualityLabel): number {
  return LABEL_TO_QN[label];
}

export function buildVideoQualityPolicy(preferredRaw: any): VideoQualityPolicy {
  const configured = normalizeVideoQualityLabel(preferredRaw);
  const preferredLabel = configured || '1080p';
  const preferredQn = qnFromVideoQualityLabel(preferredLabel);
  const minAcceptableQn = preferredQn <= 32 ? preferredQn : 64;
  return { preferredLabel, preferredQn, minAcceptableQn };
}
