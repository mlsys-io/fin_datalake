"""
We will implement the stateful UDFs for LLM inference here.
Two predictors:
1. Ray-native vllm integration
2. A custom predictor that uses only vllm model loaders and models, avoiding the overhead for serving and checks.
We hope that the second predictor will be more efficient for batch inference. 

Design:

We will keep an model actor per node, and each task will send requests to the local actor.
TODO: use Ray's placement groups to ensure that each node has exactly one model actor.
"""

import ray
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.runtime_context import get_runtime_context, RuntimeContext
from loguru import logger

class BaseModelHost:
    """
    Base per-node model host. Subclass this and implement:
      - init_model(self, **kwargs): load model(s) onto the node's GPU(s)
      - tokenize(self, batch): optional (if you want host-side tokenization)
      - infer(self, batch, **gen_params): run inference for one batch

    DO NOT override register_runner / run_registered unless you need custom behavior.
    """
    def __init__(self, model_key: str, init_kwargs: Optional[Dict[str, Any]] = None):
        self.model_key = model_key
        self.initialized = False
        self.runners: Dict[str, Callable] = {}   # name -> callable (already deserialized)
        self._init_kwargs = init_kwargs or {}

    # ---- abstract-ish; you implement these in your subclass ----
    def init_model(self, **kwargs):
        raise NotImplementedError

    def tokenize(self, batch: Any) -> Any:
        return batch  # optional override

    def infer(self, batch: Any, **gen_params) -> Any:
        raise NotImplementedError

    # ---- runner registration / execution ----
    def register_runner(self, name: str, runner_obj: Callable):
        """
        Register a callable *value* (already deserialized) into the actor once.
        'runner_obj' should be a callable with signature: runner(host, batch, **kw)
        """
        # ray will deserialize 'runner_obj' on the actor at registration time
        self.runners[name] = runner_obj

    def run_registered(self, name: str, batch, **kwargs):
        """
        Execute a previously-registered runner by name.
        'batch_ref' is expected to be an ObjectRef; we'll ray.get() inside the actor
        to zero-copy from shared memory when on the same node.
        """
        if not self.initialized:
            # Lazily init on first use with the provided kwargs from ctor
            self.init_model(**self._init_kwargs)
            self.initialized = True

        # Pull the batch locally (zero-copy when same node)
        runner = self.runners[name]
        return runner(self, batch, **kwargs)

@dataclass
class ModelHostSpec:
    host_cls: BaseModelHost                 # actor class (a ray-remote class)
    model_key: str                # logical key for this model instance (e.g., "llama31-8b")
    namespace: str = "model-hosts"
    num_cpus: int = 8
    num_gpus: float = 1
    custom_resource: Optional[str] = None 
    actor_name_prefix: str = "ModelHost"
    init_kwargs: Optional[Dict[str, Any]] = None

class ModelHostClient:
    """
    Client that (a) finds/creates a per-node host actor lazily,
    (b) allows registering named runners once,
    (c) submits batch ObjectRefs for execution.
    """
    def __init__(self, spec: ModelHostSpec):
        self.spec = spec
        self._resolved_actor = None

    def _local_node_id(self) -> str:
        return get_runtime_context().get_node_id()

    def _actor_name(self) -> str:
        # one actor per (node, model_key)
        node_id = self._local_node_id()
        return f"{self.spec.actor_name_prefix}-{self.spec.model_key}-{node_id}"

    def _get_or_create_actor(self):
        if self._resolved_actor is not None:
            return self._resolved_actor

        name = self._actor_name()
        ns = self.spec.namespace

        try:
            # Fast path: already exists on this node
            actor = ray.get_actor(name, namespace=ns)
            self._resolved_actor = actor
            return actor
        except ValueError:
            logger.info(f"Creating new model host actor {name} on node {self._local_node_id()}")

        # Create one pinned to this node
        node_id = self._local_node_id()
        opts = dict(
            name=name,
            namespace=ns,
            max_restarts=-1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False),
        )
        # resource placement: GPU + optional custom resource for co-location with Data workers
        resources = {}
        if self.spec.custom_resource:
            resources[self.spec.custom_resource] = 1
        actor = self.spec.host_cls.options(
            num_cpus=self.spec.num_cpus,
            num_gpus=self.spec.num_gpus,
            resources=resources,
            **opts
        ).remote(self.spec.model_key, self.spec.init_kwargs or {})

        self._resolved_actor = actor
        return actor

    # ---- Registration / execution API ----
    def register_runner(self, name: str, runner_callable: Callable):
        """
        Register a callable ONCE into the actor to avoid per-call pickling.
        The callable should be: runner(host: BaseModelHost, batch: Any, **kw) -> Any
        """
        actor = self._get_or_create_actor()
        # Send the callable ONCE (actor stores deserialized value)
        ray.get(actor.register_runner.remote(name, runner_callable))

    def run(self, runner_name: str, batch_ref: ray.ObjectRef, **kwargs):
        """
        Execute a registered runner by name with a batch ObjectRef.
        """
        assert isinstance(batch_ref, ray.ObjectRef), "batch_ref must be an ObjectRef"
        actor = self._get_or_create_actor()
        return actor.run_registered.remote(runner_name, batch_ref, **kwargs)

def inference_wrapper(
    spec: ModelHostSpec,
    runner_name: str,
    runner_callable: Callable,
    preprocess: Optional[Callable[[Any], Any]] = None,
    postprocess: Optional[Callable[[Any, Any], Any]] = None,
    batch_size: int = 128,
    batch_format: str = "pyarrow",     # default to pyarrow for zero-copy friendly I/O
    resources_fraction: float = 0.01,  # tiny fractional tag to co-locate with hosts
    num_cpus: int = 1,
    runner_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    A processor-like helper that:
      - lazily creates/fetches a per-node ModelHost actor
      - registers a custom runner ONCE
      - returns a dict suitable for passing to map_batches() or add_column()

    Parameters
    ----------
    spec : ModelHostSpec
        Host specification used to build/locate the per-node actor.
    runner_name : str
        Unique name for the runner inside the actor.
    runner_callable : Callable
        Callable with signature runner(host: BaseModelHost, batch, **kw) -> Any.
    preprocess : Callable[[batch], payload], optional
        Turns a batch (pyarrow.Table or pandas.DataFrame) into the payload sent to the host.
        Default: if 'text' column exists, returns list[str] from it; else raises.
    postprocess : Callable[[batch, outputs], new_batch], optional
        Builds the return batch given the original batch and model outputs.
        Default: returns {"result": outputs} or {"doc_id": ..., "result": ...} if available.
    batch_size : int
        map_batches batch size.
    batch_format : str
        "pyarrow" (default) or "pandas".
    resources_fraction : float
        Fractional custom resource on Data workers to co-locate with the per-node host.
    num_cpus : int
        CPUs per worker.
    runner_kwargs : dict
        Extra kwargs forwarded to the runner at execution time.

    Returns
    -------
    Dataset
        The resulting dataset produced by postprocess().
    """
    client = ModelHostClient(spec)
    client.register_runner(runner_name, runner_callable)
    runner_kwargs = runner_kwargs or {}

    # -------- defaults for pre/post if not provided --------
    def _default_pre(batch):
        # pyarrow.Table path
        if hasattr(batch, "column_names"):
            cols = batch.column_names
            if "content" in cols:
                return batch["content"].to_pylist()
            raise ValueError("No 'content' column found; please provide a preprocess()")
        # pandas.DataFrame path
        if hasattr(batch, "columns"):
            if "content" in batch.columns:
                return batch["content"].tolist()
            raise ValueError("No 'content' column found; please provide a preprocess()")
        return batch

    def _default_post(batch, outputs):
        return {"result": outputs}

    _pre = preprocess or _default_pre
    _post = postprocess or _default_post

    # -------- map function --------
    def _map(batch):
        # Resolve or create local actor once per worker
        if not hasattr(_map, "_client"):
            _map._client = client  # cache
        # Synchronous (simple) path
        payload = _pre(batch)
        ref = ray.put(payload)
        out_ref = _map._client.run(runner_name, ref, **runner_kwargs)
        outputs = ray.get(out_ref)
        return _post(batch, outputs)

    # -------- resources / placement --------
    resources = {}
    if spec.custom_resource:
        resources[spec.custom_resource] = resources_fraction

    return dict(
        fn=_map,
        batch_size=batch_size,
        batch_format=batch_format,
        num_cpus=num_cpus,
        num_gpus=0,          # GPU is owned by the ModelHost actor
        resources=resources if resources else None,
    )