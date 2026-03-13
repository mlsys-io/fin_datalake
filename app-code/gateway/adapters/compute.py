"""
ComputeAdapter: Domain "compute"

Handles the Compute Plane: submitting, monitoring, and cancelling ETL jobs.

Supported Actions:
    - submit_job:   Trigger a named Prefect deployment.
    - get_status:   Get status of a flow run.
    - cancel_job:   Cancel a running flow run.
    - list_jobs:    List recent flow runs.

Required Permissions:
    - submit_job / cancel_job: compute:write
    - get_status / list_jobs:  compute:read
"""

from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.models.intent import UserIntent
from gateway.models.user import Permission, User


class ComputeAdapter(BaseAdapter):

    def handles(self) -> str:
        return "compute"

    async def execute(self, user: User, intent: UserIntent) -> Any:
        dispatch = {
            "submit_job": self._submit_job,
            "get_status": self._get_status,
            "cancel_job": self._cancel_job,
            "list_jobs": self._list_jobs,
        }
        handler = dispatch.get(intent.action)
        if handler is None:
            raise ActionNotFoundError(
                f"ComputeAdapter does not support action '{intent.action}'. "
                f"Available: {list(dispatch.keys())}"
            )
        return await handler(user, intent)

    async def _submit_job(self, user: User, intent: UserIntent) -> dict:
        """Trigger a Prefect deployment. Requires compute:write."""
        self._require_permission(user, Permission.COMPUTE_WRITE)
        from prefect.deployments import run_deployment
        import asyncio
        pipeline = intent.parameters.get("pipeline")
        if not pipeline:
            raise ValueError("Parameter 'pipeline' is required.")
        params = intent.parameters.get("params", {})
        # run_deployment is synchronous, wrap in executor
        loop = asyncio.get_event_loop()
        flow_run = await loop.run_in_executor(
            None, lambda: run_deployment(name=f"{pipeline}/main", parameters=params, timeout=0)
        )
        return {"status": "submitted", "job_id": str(flow_run.id), "pipeline": pipeline}

    async def _get_status(self, user: User, intent: UserIntent) -> dict:
        """Get flow run status. Requires compute:read."""
        self._require_permission(user, Permission.COMPUTE_READ)
        from prefect.client.orchestration import get_client
        job_id = intent.parameters.get("job_id")
        if not job_id:
            raise ValueError("Parameter 'job_id' is required.")
        async with get_client() as client:
            run = await client.read_flow_run(job_id)
            return {"job_id": job_id, "status": run.state_name}

    async def _cancel_job(self, user: User, intent: UserIntent) -> dict:
        """Cancel a flow run. Requires compute:write."""
        self._require_permission(user, Permission.COMPUTE_WRITE)
        from prefect.client.orchestration import get_client
        job_id = intent.parameters.get("job_id")
        if not job_id:
            raise ValueError("Parameter 'job_id' is required.")
        async with get_client() as client:
            await client.cancel_flow_run(job_id)
            return {"job_id": job_id, "status": "cancelled"}

    async def _list_jobs(self, user: User, intent: UserIntent) -> dict:
        """List recent flow runs. Requires compute:read."""
        self._require_permission(user, Permission.COMPUTE_READ)
        from prefect.client.orchestration import get_client
        limit = intent.parameters.get("limit", 20)
        async with get_client() as client:
            runs = await client.read_flow_runs(limit=limit)
            return {"jobs": [
                {"job_id": str(r.id), "name": r.name, "status": r.state_name}
                for r in runs
            ]}
