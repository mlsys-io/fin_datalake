export function formatLocalTimestamp(
  value?: string | number | Date | null,
  options?: Intl.DateTimeFormatOptions,
): string {
  if (!value) return 'n/a'

  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) {
    return String(value)
  }

  return date.toLocaleString([], {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    ...options,
  })
}


export function formatLocalTimeOnly(value?: string | number | Date | null): string {
  return formatLocalTimestamp(value, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}
