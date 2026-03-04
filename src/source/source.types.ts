export type Platform = 'auto' | 'bilibili' | 'youtube' | 'generic';
export type ConcretePlatform = Exclude<Platform, 'auto'>;

export type MediaMode = 'both' | 'video' | 'audio';

export interface ResolveSourceInput {
  url: string;
  platform?: Platform;
  page?: number;
  title?: string;
  cookies?: string;
}

export interface SourceStreamSet {
  dashPair?: {
    videoUrl: string;
    audioUrl: string;
  };
  videoOnly?: {
    url: string;
  };
  audioOnly?: {
    url: string;
  };
  muxedBoth?: {
    url: string;
  };
}

export interface ResolvedSource {
  platform: ConcretePlatform;
  vid: string;
  canonicalUrl: string;
  title?: string;
  headers?: Record<string, string>;
  qualityMeta?: Record<string, any>;
  streams: SourceStreamSet;
}

export interface ResolvedInputIdentity {
  platform: ConcretePlatform;
  vid: string;
}

export interface SourceResolver {
  readonly platform: ConcretePlatform;
  canResolve(url: string): boolean;
  resolve(input: ResolveSourceInput): Promise<ResolvedSource>;
}

export interface PlayurlResolveInput {
  vid: string;
  page: number;
  cookies?: string;
}

export interface PlayurlResolveResult {
  cid: string | number;
  play: any;
}

export interface SourcePlayurlProvider {
  readonly platform: ConcretePlatform;
  resolvePlayurl(input: PlayurlResolveInput): Promise<PlayurlResolveResult>;
}

export interface MediaPlan {
  requested: MediaMode;
  effective: MediaMode;
  fallbackReason?: string;
}
