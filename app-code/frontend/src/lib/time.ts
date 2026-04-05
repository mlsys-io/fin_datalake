function normalizeTimestampValue(value: string | number | Date): Date {
  if (value instanceof Date) return value

  if (typeof value === 'number') {
    const milliseconds = value < 1_000_000_000_000 ? value * 1000 : value
    return new Date(milliseconds)
  }

  const trimmed = value.trim()
  if (/^\d+(\.\d+)?$/.test(trimmed)) {
    const numericValue = Number(trimmed)
    const milliseconds = numericValue < 1_000_000_000_000 ? numericValue * 1000 : numericValue
    return new Date(milliseconds)
  }

  return new Date(value)
}

export function formatLocalTimestamp(
  value?: string | number | Date | null,
  options?: Intl.DateTimeFormatOptions,
): string {
  if (!value) return 'n/a'

  const date = normalizeTimestampValue(value)
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
