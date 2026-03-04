import { CreateDownloadDto } from './dto/create-download.dto';

export const downloadSwagger = {
  post: {
    startDownload: {
      operation: {
        summary: 'Start a download job',
        description: 'Submit a URL and optional options (title, page, platform, media mode, engine) to enqueue a new download.',
      },
      body: {
        type: CreateDownloadDto,
        description: 'Download request payload. See each field description in the Schema tab.',
        required: true,
      },
    },
    resume: {
      operation: {
        summary: 'Resume a failed/stopped download',
        description: 'Signals the old job to stop/cancel and enqueues a fresh job with the same payload.',
      },
      idParam: {
        name: 'id',
        type: String,
        description: 'Existing download job id to resume from.',
        example: '123',
      },
    },
    cancel: {
      operation: {
        summary: 'Cancel a download',
        description: 'Requests cooperative cancellation and removes queued work when possible.',
      },
      idParam: {
        name: 'id',
        type: String,
        description: 'Download job id to cancel.',
        example: '123',
      },
    },
    mergePartial: {
      operation: {
        summary: 'Merge partially downloaded byte-range parts',
        description: 'Creates a partial output file from the largest contiguous set of downloaded parts.',
      },
      idParam: {
        name: 'id',
        type: String,
        description: 'Download job id that has byte-range manifest and part files.',
        example: '123',
      },
    },
  },
  get: {
    getStatus: {
      operation: {
        summary: 'Get download job status',
        description: 'Returns current queue state, progress, failure reason, result, history, and optional parts progress.',
      },
      idParam: {
        name: 'id',
        type: String,
        description: 'Download job id returned from POST /download.',
        example: '123',
      },
    },
  },
} as const;
