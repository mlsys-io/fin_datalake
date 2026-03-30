import time
import asyncio
from loguru import logger
import ray
import ray.serve as serve

from etl.agents.sentiment_agent import SentimentAgent
from etl.agents.hub import get_hub

def _kill_serve_actor(agent_name: str) -> bool:
    """
    Kill the underlying Ray Actor backing a Ray Serve deployment.

    ray.kill() requires an actual ActorHandle, not a DeploymentHandle.
    We retrieve the replica actor by its internal Serve naming convention,
    then call ray.kill() with no_restart=True so Ray does not auto-restart it.
    This correctly simulates an OOM / node-crash scenario that the Overseer
    must detect and recover from.

    Falls back to serve.delete() if the actor cannot be found (e.g. the Serve
    version uses a different naming scheme), which still removes the app from
    the Serve registry and causes requests to fail — sufficient to trigger the
    Overseer's ActorHealthPolicy.
    """
    # Ray Serve names replica actors as "<app_name>#<replica_suffix>"
    # We can retrieve the actor by the deployment app name directly.
    try:
        actor = ray.get_actor(agent_name)
        ray.kill(actor, no_restart=True)
        logger.warning(f"🔫 Killed Ray Actor backing '{agent_name}' (no_restart=True).")
        return True
    except ValueError:
        # Actor not found by that name — try the serve.delete() path
        logger.warning(f"Actor '{agent_name}' not found by name. Falling back to serve.delete().")
    except Exception as e:
        logger.warning(f"ray.kill() failed ({e}). Falling back to serve.delete().")

    try:
        serve.delete(agent_name)
        logger.warning(f"🔫 Deleted Serve app '{agent_name}' to simulate crash.")
        return True
    except Exception as e:
        logger.error(f"serve.delete() also failed: {e}")
        return False


def run_self_healing_demo():
    """
    Chaos Engineering Demo: 
    Intentionally kill a running agent and observe the Overseer detect 
    the DEAD state and Autonomically respawn it.
    """
    logger.info("=== 🛡️  OVERSEER SELF-HEALING DEMO ===")
    
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
        
    hub = get_hub()
    
    # 1. Deploy the victim agent
    agent_name = "SentimentAgent-Victim1"
    logger.info(f"[1] Deploying {agent_name}...")
    SentimentAgent.deploy(name=agent_name)
    time.sleep(2)
    
    # Get a Serve handle and verify it is alive
    handle = serve.get_app_handle(agent_name)
    if not handle:
        logger.error("Failed to deploy agent. Exiting.")
        return
        
    try:
        resp = asyncio.run(handle.ask("BTC hits all time high!"))
        logger.success(f"[1] Agent is ALIVE and responding: {resp}")
    except Exception as e:
        logger.error(f"Agent ping failed: {e}")
        return
        
    # 2. Chaos Engineering — kill the underlying Ray Actor, not the DeploymentHandle
    logger.warning(f"\n[2] Initiating Chaos Move. Killing {agent_name}...")
    killed = _kill_serve_actor(agent_name)
    if not killed:
        logger.error("Could not kill the agent. Aborting demo.")
        return

    # Verify it is actually dead (requests should now fail or time out)
    time.sleep(1)
    try:
        asyncio.run(asyncio.wait_for(handle.ask("Ping!"), timeout=1.0))
        logger.warning("Agent may still be responding — kill may not have taken effect yet.")
    except Exception:
        logger.success("[2] Confirmed: Agent is DEAD. Requests are timing out.")
        
    # 3. Wait for Overseer MAPE-K Loop to detect and recover
    logger.info("\n[3] Waiting for Overseer Autonomic Recovery...")
    logger.info("(Ensure the Overseer is running in another terminal window!)")
    logger.info("The ActorHealthPolicy will detect the DEAD state and trigger RayActuator.")
    
    start_wait = time.perf_counter()
    recovered = False
    
    # Poll every second for up to 45 seconds (Overseer loop is 5–10s)
    for attempt in range(45):
        time.sleep(1)
        if attempt % 5 == 0:
            logger.debug(f"... polling AgentHub (Attempt {attempt})...")
            
        # The Overseer re-registers the new instance under the same capability
        agent_names = ray.get(hub.find_by_capability.remote("sentiment"))
        if agent_names:
            try:
                new_handle = serve.get_app_handle(agent_names[0])
                resp = asyncio.run(asyncio.wait_for(new_handle.ask("Markets drop sharply"), timeout=2.0))
                mttr = time.perf_counter() - start_wait
                logger.success(f"\n[4] 🟢 RECOVERY SUCCESSFUL! MTTR: {mttr:.1f} seconds")
                logger.success(f"New agent handle responding normally: {resp}")
                recovered = True
                break
            except Exception:
                pass
                
    if not recovered:
        logger.error("\n❌ Recovery timed out. Is the Overseer process running?")

if __name__ == "__main__":
    try:
        run_self_healing_demo()
    except KeyboardInterrupt:
        logger.info("Demo interrupted.")
