export const createDownloadDtoSwagger = {
  url: {
    description: 'Public video URL to download (Bilibili link or similar)',
    example: 'https://www.bilibili.com/video/BV1xx411c7mD',
    required: true,
  },
  title: {
    description: 'Optional title to save the video under',
    example: 'My favorite clip',
  },
  p: {
    description: 'Optional page/part number for multi-part Bilibili videos (1-based)',
    example: 2,
    minimum: 1,
  },
  platform: {
    description: 'Optional platform override. Default is auto-detection from URL.',
    enum: ['auto', 'bilibili', 'youtube', 'generic'],
    default: 'auto',
  },
  media: {
    description: 'Select which media tracks to output. Default is both tracks.',
    enum: ['both', 'video', 'audio'],
    default: 'both',
  },
  engine: {
    description: 'Download engine strategy. auto tries native first then yt-dlp fallback.',
    enum: ['auto', 'yt-dlp', 'native'],
    default: 'auto',
  },
} as const;
