"""
System Overseer — Autonomic monitoring service for the AI Lakehouse.

The Overseer runs as a standalone process, independent of Ray,
and implements the MAPE-K control loop:
  Monitor → Analyze → Plan → Execute

It probes all managed services (Ray, Kafka, Prefect, etc.) via their
native APIs and applies policies to auto-scale agents or self-heal.
"""
