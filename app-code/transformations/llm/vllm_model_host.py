import ray
from .base import BaseModelHost
from vllm import LLM, SamplingParams, envs
from typing import Any, Dict, List, Union, Optional

# @ray.remote(num_gpus=1)
@ray.remote
class VLLMModelHost(BaseModelHost):
    """
    vLLM-backed model host.
    - Lazily initializes a vLLM engine on first use.
    - Accepts either plain string prompts or chat 'messages' (list[dict]).
    - 'infer' returns a list[str] aligned to the input batch order.
    """

    def init_model(
        self,
        model: str,
        dtype: str = "bfloat16",
        tensor_parallel_size: int = 1,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        **kw,
    ):
        engine_kwargs = engine_kwargs or {}
        envs.set_vllm_use_v1(False) 
        assert envs.VLLM_USE_V1 == False, "VLLM_USE_V1 must be set to 0/False to use the vLLM 2.x API"
        self.llm = LLM(
            model=model,
            dtype=dtype,
            tensor_parallel_size=tensor_parallel_size,
            **engine_kwargs,
        )

    def tokenize(
        self, batch: Union[List[str], List[List[Dict[str, str]]]]
    ) -> List[str]:
        """
        Normalize inputs to plain prompt strings.
        Supports:
          - list[str] prompts
          - list[list[{"role": ..., "content": ...}]] chat messages
        """
        assert isinstance(batch, list) and batch, "Batch must be a non-empty list"

        first = batch[0]
        if isinstance(first, str):
            return batch  # type: ignore

        if isinstance(first, list) and first and isinstance(first[0], dict):
            tokenizer = self.llm.get_tokenizer()
            return [tokenizer.apply_chat_template(msgs, add_generation_prompt=True, tokenize=False) for msgs in batch]

        raise TypeError("Unsupported batch format for VLLMModelHost")

    def infer(
        self,
        batch: List[str],
        **gen_params,
    ) -> List[str]:
        """
        Run generation.
        gen_params may include: max_tokens, temperature, top_p/top_k, stop, etc.
        Returns list[str] of generated texts (one per input).
        """

        sp = gen_params if isinstance(gen_params, dict) else dict(gen_params)
        sp.setdefault("max_tokens", 128)
        sampling = SamplingParams(**sp)
        outputs = self.llm.generate(batch, sampling_params=sampling)
        return [o.outputs[0].text if o.outputs else "" for o in outputs]
