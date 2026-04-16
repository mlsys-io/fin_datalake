export type BenchmarkStage = {
  label: string
  seconds: number
}

export type BenchmarkSystemEvidence = {
  id: 'integrated' | 'spark_glue' | 'plain_sequential'
  label: string
  role: 'proposed' | 'baseline'
  trials: number
  successCount: number
  meanTotalSeconds: number
  stdTotalSeconds: number
  medianTotalSeconds: number
  p95TotalSeconds: number
  meanSignalSeconds: number
  stageBreakdown: BenchmarkStage[]
  weakness: string
  interpretation: string
}

export type ZeroCopyEvidence = {
  rows: number
  serializedMeanMs: number
  serializedStdMs: number
  serializedPeakMemMb: number
  zeroCopyMeanMs: number
  zeroCopyStdMs: number
  zeroCopyPeakMemMb: number
  speedup: number
}

export type RecoveryEvidence = {
  mode: 'overseer' | 'manual'
  label: string
  trials: number
  successes: number
  meanMttrSeconds: number
  stdSeconds: number
  p95Seconds: number
  summary: string
}

export type BenchmarkEvidenceData = {
  marketPulse: BenchmarkSystemEvidence[]
  zeroCopy: ZeroCopyEvidence[]
  recovery: RecoveryEvidence[]
  caveats: string[]
  sourceNotes: string[]
}

export const benchmarkEvidenceData: BenchmarkEvidenceData = {
  marketPulse: [
    {
      id: 'integrated',
      label: 'Integrated Market Pulse',
      role: 'proposed',
      trials: 5,
      successCount: 5,
      meanTotalSeconds: 30.469623394683005,
      stdTotalSeconds: 1.7441080983573438,
      medianTotalSeconds: 30.25605912320316,
      p95TotalSeconds: 33.034191804006696,
      meanSignalSeconds: 1.0235072873532771,
      stageBreakdown: [
        { label: 'Ingest', seconds: 13.175504901260137 },
        { label: 'Agent setup', seconds: 13.90073370113969 },
        { label: 'Signal', seconds: 1.0235072873532771 },
        { label: 'Persistence', seconds: 0.2587929252535105 },
        { label: 'Visibility', seconds: 0.5675237189978362 },
      ],
      weakness: 'None for this workload shape; this is the reference design.',
      interpretation: 'The signal stage stays short because the agents, orchestration, persistence, and visibility all sit on the same platform contract.',
    },
    {
      id: 'spark_glue',
      label: 'Spark + Glue',
      role: 'baseline',
      trials: 5,
      successCount: 5,
      meanTotalSeconds: 48.38371748998761,
      stdTotalSeconds: 9.834690250313097,
      medianTotalSeconds: 46.73620295338333,
      p95TotalSeconds: 63.98608283139765,
      meanSignalSeconds: 37.95097845196724,
      stageBreakdown: [
        { label: 'Ingest', seconds: 7.87562290020287 },
        { label: 'Signal', seconds: 37.95097845196724 },
        { label: 'Persistence', seconds: 0.20914659984409809 },
      ],
      weakness: 'The compute side is scalable, but the intelligence path still feels stitched together.',
      interpretation: 'The signal stage remains large, which is the clearest sign of handoff overhead for this workload.',
    },
    {
      id: 'plain_sequential',
      label: 'Plain Sequential',
      role: 'baseline',
      trials: 5,
      successCount: 5,
      meanTotalSeconds: 55.34108367972076,
      stdTotalSeconds: 39.4588452093548,
      medianTotalSeconds: 42.41187244839966,
      p95TotalSeconds: 122.17916700430214,
      meanSignalSeconds: 49.139554276689886,
      stageBreakdown: [
        { label: 'Ingest', seconds: 5.963924681767821 },
        { label: 'Signal', seconds: 49.139554276689886 },
        { label: 'Persistence', seconds: 0.23760184794664382 },
      ],
      weakness: 'Everything runs through one blocking path, so there is no durable separation of concerns.',
      interpretation: 'The high variance and long signal stage make the fragility of a sequential baseline very easy to see.',
    },
  ],
  zeroCopy: [
    {
      rows: 10000,
      serializedMeanMs: 524.0197147553166,
      serializedStdMs: 215.3306948283809,
      serializedPeakMemMb: 5.419283231099446,
      zeroCopyMeanMs: 60.65037275354067,
      zeroCopyStdMs: 11.877451130152869,
      zeroCopyPeakMemMb: 1.3818819999694825,
      speedup: 8.640008147760726,
    },
    {
      rows: 100000,
      serializedMeanMs: 3660.8832207818828,
      serializedStdMs: 115.08981888207997,
      serializedPeakMemMb: 53.806755701700844,
      zeroCopyMeanMs: 264.121066344281,
      zeroCopyStdMs: 15.439105392412234,
      zeroCopyPeakMemMb: 13.741507085164388,
      speedup: 13.860625626923422,
    },
    {
      rows: 500000,
      serializedMeanMs: 17594.120043391984,
      serializedStdMs: 283.9786867219349,
      serializedPeakMemMb: 254.35746835072834,
      zeroCopyMeanMs: 1176.1723902076483,
      zeroCopyStdMs: 32.081179428540196,
      zeroCopyPeakMemMb: 55.784004592895506,
      speedup: 14.958793617222911,
    },
    {
      rows: 1000000,
      serializedMeanMs: 35135.60603993634,
      serializedStdMs: 1327.6475484737048,
      serializedPeakMemMb: 509.9392562866211,
      zeroCopyMeanMs: 2338.788313791156,
      zeroCopyStdMs: 41.318106309139154,
      zeroCopyPeakMemMb: 101.56129290262858,
      speedup: 15.022995383016013,
    },
  ],
  recovery: [
    {
      mode: 'overseer',
      label: 'Overseer',
      trials: 30,
      successes: 30,
      meanMttrSeconds: 4.607650421621899,
      stdSeconds: 0.10187113923731873,
      p95Seconds: 4.747218003496528,
      summary: 'Immediate control-plane intervention removes operator reaction time and keeps the recovery path consistent.',
    },
    {
      mode: 'manual',
      label: 'Manual',
      trials: 30,
      successes: 30,
      meanMttrSeconds: 13.38866267496099,
      stdSeconds: 3.0594834326432063,
      p95Seconds: 18.00899103656411,
      summary: 'The same service eventually comes back, but human delay makes the recovery path slower and more variable.',
    },
  ],
  caveats: [
    'This is a workload-specific comparison, not a claim about every possible data platform.',
    'The Market Pulse numbers are repeated-trial artifacts, not live benchmark execution during the presentation.',
    'The zero-copy benchmark isolates handoff overhead; it is not a full application benchmark.',
    'MTTR measures time to usable service, and the catalog can lag briefly behind endpoint recovery.',
  ],
  sourceNotes: [
    'Market Pulse comparative benchmark artifact set: 20260407-233742',
    'Zero-copy handoff artifact set: 20260406-223020',
    'MTTR recovery artifact set: 20260407-034737',
  ],
}

export function formatSeconds(value: number): string {
  return `${value.toFixed(2)}s`
}

export function formatMilliseconds(value: number): string {
  return `${value.toFixed(2)}ms`
}

export function formatRatio(value: number): string {
  return `${value.toFixed(2)}x`
}

