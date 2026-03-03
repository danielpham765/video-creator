export function formatElapsedDuration(ms: number): string {
  const totalSec = Math.max(0, Math.floor((Number.isFinite(ms) ? ms : 0) / 1000));
  const hours = Math.floor(totalSec / 3600);
  const minutes = Math.floor((totalSec % 3600) / 60);
  const seconds = totalSec % 60;

  if (hours > 0) return `${hours} h - ${minutes} min - ${seconds} sec`;
  if (minutes > 0) return `${minutes} min - ${seconds} sec`;
  return `${seconds} sec`;
}
