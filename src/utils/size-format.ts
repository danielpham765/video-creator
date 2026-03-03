const BYTES_PER_MB = 1024 * 1024;

export function formatMb(bytes: number): string {
  const safeBytes = Number.isFinite(bytes) ? Math.max(0, bytes) : 0;
  const mb = safeBytes / BYTES_PER_MB;
  return mb.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export function formatMbProgress(downloadedBytes: number, totalBytes: number): string {
  return `${formatMb(downloadedBytes)} / ${formatMb(totalBytes)} MB`;
}
