import React from 'react'
import { BarChart3, ShieldCheck, TrendingUp } from 'lucide-react'
import { ViewShell } from '../shared/ViewShell'
import {
  benchmarkEvidenceData,
  formatMilliseconds,
  formatRatio,
  formatSeconds,
  type BenchmarkSystemEvidence,
  type RecoveryEvidence,
  type ZeroCopyEvidence,
} from '../../data/benchmarkEvidence'

const stageLabels = ['Ingest', 'Agent setup', 'Signal', 'Persistence', 'Visibility'] as const

function getStageValue(system: BenchmarkSystemEvidence, label: string): number | null {
  return system.stageBreakdown.find(stage => stage.label === label)?.seconds ?? null
}

function stageTone(system: BenchmarkSystemEvidence, label: string): string {
  if (label === 'Signal') {
    return system.id === 'integrated'
      ? 'bg-emerald-500'
      : system.id === 'spark_glue'
        ? 'bg-amber-500'
        : 'bg-rose-500'
  }

  switch (label) {
    case 'Ingest': return 'bg-stone-500'
    case 'Agent setup': return 'bg-sky-500'
    case 'Persistence': return 'bg-stone-400'
    case 'Visibility': return 'bg-cyan-500'
    default: return 'bg-stone-500'
  }
}

function RolePill({ role }: { role: BenchmarkSystemEvidence['role'] }) {
  return (
    <span className={`rounded-md px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] ${
      role === 'proposed' ? 'bg-emerald-50 text-emerald-700' : 'bg-stone-100 text-stone-700'
    }`}>
      {role === 'proposed' ? 'Reference' : 'Baseline'}
    </span>
  )
}

function MetricTile({
  label,
  value,
  helper,
}: {
  label: string
  value: string
  helper?: string
}) {
  return (
    <div className="rounded-lg border border-stone-200 bg-stone-50 px-3 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-400">{label}</p>
      <p className="mt-2 font-mono text-lg text-stone-900">{value}</p>
      {helper && <p className="mt-1 text-xs text-stone-500">{helper}</p>}
    </div>
  )
}

function StageBreakdownBar({ system, maxTotalSeconds }: { system: BenchmarkSystemEvidence; maxTotalSeconds: number }) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-3 text-xs text-stone-500">
        <p className="font-medium text-stone-900">{system.label}</p>
        <p className="font-mono">{formatSeconds(system.meanTotalSeconds)}</p>
      </div>
      <div className="h-4 overflow-hidden rounded-md border border-stone-200 bg-stone-100">
        <div className="flex h-full">
          {stageLabels.map(label => {
            const value = getStageValue(system, label)
            if (!value) return null
            const width = Math.max((value / maxTotalSeconds) * 100, 0.15)
            return (
              <div
                key={`${system.id}-${label}`}
                className={stageTone(system, label)}
                style={{ width: `${width}%` }}
                title={`${label}: ${formatSeconds(value)}`}
              />
            )
          })}
        </div>
      </div>
      <div className="flex flex-wrap gap-2 text-[11px] text-stone-500">
        {system.stageBreakdown.map(stage => (
          <span key={stage.label} className="inline-flex items-center gap-1 rounded-md border border-stone-200 bg-white px-2 py-1">
            <span className={`h-2 w-2 rounded-full ${stageTone(system, stage.label)}`} />
            {stage.label} {formatSeconds(stage.seconds)}
          </span>
        ))}
      </div>
    </div>
  )
}

function ComparisonBar({ leftLabel, rightLabel, leftValue, rightValue, leftTone, rightTone }: {
  leftLabel: string
  rightLabel: string
  leftValue: number
  rightValue: number
  leftTone: string
  rightTone: string
}) {
  const total = leftValue + rightValue
  const leftWidth = total === 0 ? 50 : (leftValue / total) * 100
  const rightWidth = total === 0 ? 50 : (rightValue / total) * 100

  return (
    <div className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm">
      <div className="flex items-center justify-between gap-3 text-sm">
        <p className="font-medium text-stone-900">{leftLabel}</p>
        <p className="font-mono text-stone-500">{formatSeconds(leftValue)} vs {formatSeconds(rightValue)}</p>
      </div>
      <div className="mt-3 h-4 overflow-hidden rounded-md border border-stone-200 bg-stone-100">
        <div className="flex h-full">
          <div className={leftTone} style={{ width: `${leftWidth}%` }} />
          <div className={rightTone} style={{ width: `${rightWidth}%` }} />
        </div>
      </div>
      <div className="mt-2 flex justify-between text-xs text-stone-500">
        <span>{leftLabel}</span>
        <span>{rightLabel}</span>
      </div>
    </div>
  )
}

function ZeroCopyRow({ row }: { row: ZeroCopyEvidence }) {
  return (
    <tr className="align-top">
      <td className="px-4 py-3 font-medium text-stone-900">{row.rows.toLocaleString()}</td>
      <td className="px-4 py-3 font-mono text-stone-700">{formatMilliseconds(row.serializedMeanMs)}</td>
      <td className="px-4 py-3 font-mono text-stone-700">{formatMilliseconds(row.zeroCopyMeanMs)}</td>
      <td className="px-4 py-3 font-mono text-stone-700">{formatRatio(row.speedup)}</td>
    </tr>
  )
}

function RecoveryCard({ evidence, tone }: { evidence: RecoveryEvidence; tone: string }) {
  return (
    <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{evidence.label}</p>
          <p className="mt-2 text-3xl font-bold text-stone-900">{formatSeconds(evidence.meanMttrSeconds)}</p>
          <p className="mt-1 text-sm text-stone-500">Mean MTTR</p>
        </div>
        <span className={`rounded-md px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] ${tone}`}>
          {evidence.successes}/{evidence.trials}
        </span>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-3">
        <MetricTile label="Std dev" value={formatSeconds(evidence.stdSeconds)} />
        <MetricTile label="P95" value={formatSeconds(evidence.p95Seconds)} />
      </div>
      <p className="mt-4 text-sm leading-6 text-stone-600">{evidence.summary}</p>
    </div>
  )
}

export const BenchmarkEvidence: React.FC = () => {
  const systems = benchmarkEvidenceData.marketPulse
  const maxTotalSeconds = Math.max(...systems.map(system => system.meanTotalSeconds))
  const integrated = systems.find(system => system.id === 'integrated')
  const spark = systems.find(system => system.id === 'spark_glue')
  const plain = systems.find(system => system.id === 'plain_sequential')
  const zeroCopyHeadline = benchmarkEvidenceData.zeroCopy.at(-1)
  const overseer = benchmarkEvidenceData.recovery.find(item => item.mode === 'overseer')
  const manual = benchmarkEvidenceData.recovery.find(item => item.mode === 'manual')

  const sparkRatio = integrated && spark ? spark.meanTotalSeconds / integrated.meanTotalSeconds : 0
  const plainRatio = integrated && plain ? plain.meanTotalSeconds / integrated.meanTotalSeconds : 0
  const mttrRatio = overseer && manual ? manual.meanMttrSeconds / overseer.meanMttrSeconds : 0

  return (
    <ViewShell
      eyebrow="Showcase"
      title="Baseline Evidence"
      description="Measured artifacts kept short so the dashboard stays focused on the result, not the whole comparison story."
    >
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(0,0.82fr)_minmax(0,0.82fr)]">
        <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Headline Result</p>
              <h3 className="mt-2 text-2xl font-bold text-stone-900">{integrated?.label}</h3>
              <p className="mt-1 text-sm text-stone-500">5 fully live trials</p>
            </div>
            {integrated && <RolePill role={integrated.role} />}
          </div>

          <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <MetricTile label="Mean total" value={integrated ? formatSeconds(integrated.meanTotalSeconds) : 'n/a'} helper="Best overall runtime" />
            <MetricTile label="Signal stage" value={integrated ? formatSeconds(integrated.meanSignalSeconds) : 'n/a'} helper="Short and stable" />
            <MetricTile label="Std dev" value={integrated ? formatSeconds(integrated.stdTotalSeconds) : 'n/a'} helper="Low spread" />
            <MetricTile label="P95" value={integrated ? formatSeconds(integrated.p95TotalSeconds) : 'n/a'} helper="Tight upper bound" />
          </div>

          <div className="mt-4 rounded-lg border border-stone-200 bg-stone-50 px-4 py-3 text-sm text-stone-600">
            {integrated?.interpretation}
          </div>
        </div>

        <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Comparison</p>
          <div className="mt-4 space-y-3">
            <MetricTile
              label="Spark + Glue"
              value={spark ? formatSeconds(spark.meanTotalSeconds) : 'n/a'}
              helper={spark ? `${formatRatio(sparkRatio)} slower than integrated` : undefined}
            />
            <MetricTile
              label="Plain Sequential"
              value={plain ? formatSeconds(plain.meanTotalSeconds) : 'n/a'}
              helper={plain ? `${formatRatio(plainRatio)} slower than integrated` : undefined}
            />
            <MetricTile
              label="All systems"
              value="15 trials"
              helper="Each system succeeded"
            />
          </div>
        </div>

        <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">What To Notice</p>
          <div className="mt-4 space-y-3 text-sm text-stone-600">
            <p>The integrated system keeps the signal stage tiny because the agents already live inside the platform.</p>
            <p>Spark + Glue still spends most of its time in the signal path.</p>
            <p>Plain sequential shows the biggest variance and the least architectural separation.</p>
          </div>
        </div>
      </div>

      <section className="space-y-4">
        <div className="flex items-center gap-2">
          <BarChart3 size={16} className="text-stone-500" />
          <h3 className="text-sm font-semibold text-stone-900">Stage Breakdown</h3>
        </div>
        <div className="space-y-5 rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          {systems.map(system => (
            <StageBreakdownBar key={system.id} system={system} maxTotalSeconds={maxTotalSeconds} />
          ))}
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-3">
        {systems.map(system => (
          <div key={system.id} className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">{system.label}</p>
                <p className="mt-2 text-lg font-bold text-stone-900">{system.weakness}</p>
              </div>
              <RolePill role={system.role} />
            </div>
            <p className="mt-4 text-sm leading-6 text-stone-600">{system.interpretation}</p>
            <div className="mt-4 grid grid-cols-2 gap-3">
              <MetricTile label="Mean total" value={formatSeconds(system.meanTotalSeconds)} />
              <MetricTile label="Std dev" value={formatSeconds(system.stdTotalSeconds)} />
            </div>
          </div>
        ))}
      </section>

      <section className="space-y-4">
        <div className="flex items-center gap-2">
          <TrendingUp size={16} className="text-stone-500" />
          <h3 className="text-sm font-semibold text-stone-900">Zero-Copy Evidence</h3>
        </div>
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
          <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <div className="overflow-hidden rounded-lg border border-stone-200">
              <table className="w-full text-left text-sm">
                <thead className="bg-stone-50 text-[11px] uppercase tracking-[0.16em] text-stone-400">
                  <tr>
                    <th className="px-4 py-3">Rows</th>
                    <th className="px-4 py-3">Serialized</th>
                    <th className="px-4 py-3">Zero-copy</th>
                    <th className="px-4 py-3">Speedup</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-stone-100">
                  {benchmarkEvidenceData.zeroCopy.map(row => (
                    <ZeroCopyRow key={row.rows} row={row} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="space-y-4">
            <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
              <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Headline Speedup</p>
              <p className="mt-2 text-3xl font-bold text-stone-900">
                {zeroCopyHeadline ? formatRatio(zeroCopyHeadline.speedup) : 'n/a'}
              </p>
              <p className="mt-1 text-sm text-stone-500">
                At {zeroCopyHeadline ? zeroCopyHeadline.rows.toLocaleString() : 'n/a'} rows
              </p>
              <div className="mt-4 grid grid-cols-2 gap-3">
                <MetricTile
                  label="Serialized peak memory"
                  value={zeroCopyHeadline ? `${zeroCopyHeadline.serializedPeakMemMb.toFixed(2)} MB` : 'n/a'}
                />
                <MetricTile
                  label="Zero-copy peak memory"
                  value={zeroCopyHeadline ? `${zeroCopyHeadline.zeroCopyPeakMemMb.toFixed(2)} MB` : 'n/a'}
                />
              </div>
            </div>

            {zeroCopyHeadline && (
              <ComparisonBar
                leftLabel="Serialized"
                rightLabel="Zero-copy"
                leftValue={zeroCopyHeadline.serializedMeanMs}
                rightValue={zeroCopyHeadline.zeroCopyMeanMs}
                leftTone="bg-stone-400"
                rightTone="bg-emerald-500"
              />
            )}
          </div>
        </div>
      </section>

      <section className="space-y-4">
        <div className="flex items-center gap-2">
          <ShieldCheck size={16} className="text-stone-500" />
          <h3 className="text-sm font-semibold text-stone-900">Recovery Evidence</h3>
        </div>
        <div className="grid gap-4 xl:grid-cols-[minmax(0,0.92fr)_minmax(0,0.92fr)_minmax(0,1.16fr)]">
          {benchmarkEvidenceData.recovery.map(item => (
            <RecoveryCard
              key={item.mode}
              evidence={item}
              tone={item.mode === 'overseer' ? 'bg-emerald-50 text-emerald-700' : 'bg-stone-100 text-stone-700'}
            />
          ))}

          <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Comparison</p>
            <p className="mt-2 text-3xl font-bold text-stone-900">
              {formatRatio(mttrRatio)}
            </p>
            <p className="mt-1 text-sm text-stone-500">Manual recovery is slower</p>
            <div className="mt-4 h-4 overflow-hidden rounded-md border border-stone-200 bg-stone-100">
              <div className="flex h-full">
                {overseer && manual && (
                  <>
                    <div className="bg-emerald-500" style={{ width: `${(overseer.meanMttrSeconds / (overseer.meanMttrSeconds + manual.meanMttrSeconds)) * 100}%` }} />
                    <div className="bg-rose-500" style={{ width: `${(manual.meanMttrSeconds / (overseer.meanMttrSeconds + manual.meanMttrSeconds)) * 100}%` }} />
                  </>
                )}
              </div>
            </div>
            <div className="mt-3 flex justify-between text-xs text-stone-500">
              <span>Overseer</span>
              <span>Manual</span>
            </div>
            <div className="mt-4 rounded-lg border border-stone-200 bg-stone-50 px-4 py-3 text-sm text-stone-600">
              The control loop keeps recovery consistent by removing operator reaction time.
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
        <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Caveats</p>
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {benchmarkEvidenceData.caveats.map(item => (
              <div key={item} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-sm text-stone-600">
                {item}
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-lg border border-stone-200 bg-white p-5 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-400">Source Notes</p>
          <div className="mt-4 space-y-2 text-sm text-stone-600">
            {benchmarkEvidenceData.sourceNotes.map(item => (
              <p key={item} className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2">
                {item}
              </p>
            ))}
          </div>
        </div>
      </section>
    </ViewShell>
  )
}
