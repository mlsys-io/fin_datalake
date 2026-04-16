export function sanitizeSqlIdentifier(value?: string): string {
  const candidate = (value ?? '').trim()
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(candidate) ? candidate : 'public'
}

export function getRisingWaveSchema(): string {
  return sanitizeSqlIdentifier((import.meta.env.VITE_RISINGWAVE_SCHEMA as string | undefined) ?? 'public')
}
