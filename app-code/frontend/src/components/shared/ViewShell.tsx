import React from 'react'

type ViewShellProps = {
  eyebrow?: string
  title: string
  description: string
  actions?: React.ReactNode
  children: React.ReactNode
}

export const ViewShell: React.FC<ViewShellProps> = ({ eyebrow, title, description, actions, children }) => {
  return (
    <section className="mx-auto flex w-full max-w-7xl flex-col gap-6 pb-12">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-stone-200 pb-4">
        <div className="min-w-0 space-y-2">
          {eyebrow && (
            <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-stone-500">
              {eyebrow}
            </p>
          )}
          <div className="min-w-0">
            <h2 className="text-2xl font-bold text-stone-900">{title}</h2>
            <p className="mt-1 max-w-3xl text-sm text-stone-500">{description}</p>
          </div>
        </div>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </header>
      {children}
    </section>
  )
}
