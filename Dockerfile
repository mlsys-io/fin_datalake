ARG RAY_TAG=2.49.2-py312-cu128
FROM rayproject/ray:${RAY_TAG}

# Use bash -lc so conda works during build
SHELL ["/bin/bash", "-lc"]

# Root: OS deps for CPU build of vLLM, tcmalloc, etc.
USER root
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      git build-essential gcc-12 g++-12 cmake ninja-build libnuma-dev numactl \
      libtcmalloc-minimal4 curl ca-certificates \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 10 \
         --slave /usr/bin/g++ g++ /usr/bin/g++-12 \
    && rm -rf /var/lib/apt/lists/*

# Make sure conda is on PATH for both common Ray image layouts
ENV CONDA_HOME=/home/ray/anaconda3
ENV PATH=${CONDA_HOME}/bin:/opt/conda/bin:$PATH

# Switch to ray before creating envs so ownership is correct
USER ray

# -------------------------------------------------------
# Create two conda envs (both py312) and install packages
# -------------------------------------------------------
RUN conda create -y -n py-gpu --clone base && \
    conda create -y -n py-cpu --clone base && \
    conda clean -afy

# GPU env: vLLM GPU wheel (CUDA 12.8) + your extras
ARG EXTRA_PY="deltalake thrift==0.22.0 fsspec==2025.9.0 loguru beautifulsoup4 lxml"
RUN conda run -n py-gpu pip install --upgrade pip && \
    conda run -n py-gpu pip install vllm --extra-index-url https://download.pytorch.org/whl/cu128 && \
    conda run -n py-gpu pip install ${EXTRA_PY}

# CPU env: build vLLM for CPU + your extras
RUN git clone --depth=1 https://github.com/vllm-project/vllm /tmp/vllm && \
    conda run -n py-cpu pip install -r /tmp/vllm/requirements/cpu-build.txt --extra-index-url https://download.pytorch.org/whl/cpu && \
    conda run -n py-cpu pip install -r /tmp/vllm/requirements/cpu.txt        --extra-index-url https://download.pytorch.org/whl/cpu && \
    conda run -n py-cpu bash -lc 'VLLM_TARGET_DEVICE=cpu pip install -v /tmp/vllm --no-build-isolation --no-deps' && \
    rm -rf /tmp/vllm && \
    conda run -n py-cpu pip install ${EXTRA_PY} && \
    conda clean -afy

# -------------------------------------------------------
# Unified selection wrappers (CPU/GPU auto-pick)
# -------------------------------------------------------
RUN tee /home/ray/_pick_conda_env >/dev/null <<'SH'
#!/usr/bin/env bash
set -euo pipefail
want="${VLLM_DEVICE:-auto}"
pick="py-cpu"

if [[ "$want" == "gpu" ]]; then
  pick="py-gpu"
elif [[ "$want" == "cpu" ]]; then
  pick="py-cpu"
else
  # auto: prefer GPU if visible & not masked
  if command -v nvidia-smi >/dev/null 2>&1; then
    if nvidia-smi -L >/dev/null 2>&1 && [[ "${NVIDIA_VISIBLE_DEVICES:-}" != "void" && "${NVIDIA_VISIBLE_DEVICES:-}" != "none" ]]; then
      pick="py-gpu"
    fi
  fi
fi

echo "Picked conda env: $pick"

# CPU path gets tcmalloc for better allocator behavior
if [[ "$pick" == "py-cpu" ]]; then
  export LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4${LD_PRELOAD:+:$LD_PRELOAD}"
fi

# Pass through all args to conda-run in the selected env
exec conda run -n "$pick" --no-capture-output "$@"
SH
RUN chmod +x /home/ray/_pick_conda_env

RUN printf '%s\n' '#!/usr/bin/env bash' 'exec /home/ray/_pick_conda_env python "$@"' > /home/ray/anaconda3/bin/env-python && chmod +x /home/ray/anaconda3/bin/env-python && \
    printf '%s\n' '#!/usr/bin/env bash' 'exec /home/ray/_pick_conda_env pip "$@"'    > /home/ray/anaconda3/bin/env-pip    && chmod +x /home/ray/anaconda3/bin/env-pip    && \
    printf '%s\n' '#!/usr/bin/env bash' 'exec /home/ray/_pick_conda_env "$@"'        > /home/ray/anaconda3/bin/env-exec   && chmod +x /home/ray/anaconda3/bin/env-exec