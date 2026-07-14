import time
from typing import TypedDict, Optional
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver


# 1. SCHEMAS & GATEWAY SIMULATION

class ErrorCodes:
    API_TIMEOUT = "ERR_EXT_408"
    SYSTEM_EXHAUSTED = "ERR_SYS_500"

class AgentState(TypedDict):
    user_id: str
    action: str
    amount: int
    error_count: int
    result: Optional[str]
    is_high_risk: bool
    is_approved: bool
    last_error_code: Optional[str]
    retry_delay_sec: int

HIGH_RISK_ACTIONS = {"issue_refund", "delete_account", "send_bulk_email"}

def call_external_gateway(action: str, amount: int, current_attempts: int) -> str:
    # Fails on attempt 1 (0) and attempt 2 (1). Succeeds on attempt 3 (2).
    if current_attempts < 2:
        raise ConnectionResetError("Remote gateway closed connection prematurely.")
    if action == "issue_refund":
        return f"SUCCESS: Transferred ${amount} to customer ledger."
    return f"SUCCESS: Action '{action}' committed."


# 2. DEFINITIVE STATE NODES

def intake_node(state: AgentState) -> dict:
    print(f"\n[NODE -> INTAKE] Analyzing risk profile for user: {state['user_id']} | Action: {state['action']}")
    is_high_risk = state["action"] in HIGH_RISK_ACTIONS
    return {"is_high_risk": is_high_risk, "is_approved": False}

def execution_node(state: AgentState) -> dict:
    current_attempt = state.get("error_count", 0)
    print(f"[NODE -> EXECUTION] Accessing API gateway. Attempt number: {current_attempt + 1}")
    
    try:
        api_result = call_external_gateway(state["action"], state.get("amount", 0), current_attempt)
        print(f"🎉 API SUCCESS: Received payload -> {api_result}")
        return {
            "result": api_result,
            "error_count": current_attempt,
            "last_error_code": None,
            "retry_delay_sec": 0
        }
    except Exception as error:
        next_attempt = current_attempt + 1
        next_delay = 2 ** next_attempt 
        print(f"❌ ERROR: Downstream API failed on attempt {next_attempt} ({str(error)})")
        return {
            "result": None,
            "error_count": next_attempt, 
            "last_error_code": ErrorCodes.API_TIMEOUT, 
            "retry_delay_sec": next_delay
        }

def backoff_delay_node(state: AgentState) -> dict:
    delay = state.get("retry_delay_sec", 2)
    print(f"⏳ [BACKOFF ACTIVE] System throttling loop. Pausing execution thread for {delay} seconds...")
    time.sleep(delay)
    return {}

def circuit_breaker_node(state: AgentState) -> dict:
    print(f"🚨 [CIRCUIT BREAKER] Open circuit condition met. All retries exhausted.")
    return {"result": f"Aborted. Code: {ErrorCodes.SYSTEM_EXHAUSTED}.", "last_error_code": ErrorCodes.SYSTEM_EXHAUSTED}


# 3. ROUTERS & CONDITIONAL EDGES

def route_risk_assessment(state: AgentState) -> str:
    if state["is_high_risk"] and not state["is_approved"]:
        return "pending_approval"
    return "execute"

def route_execution_outcome(state: AgentState) -> str:
    if state.get("result") is not None:
        return "exit"
    if state.get("error_count", 0) >= 3:
        return "trip_breaker"
    return "retry_loop"


# 4. GRAPH COMPILATION

builder = StateGraph(AgentState)
builder.add_node("intake", intake_node)
builder.add_node("execution", execution_node)
builder.add_node("backoff_delay", backoff_delay_node)
builder.add_node("circuit_breaker", circuit_breaker_node)

builder.set_entry_point("intake")
builder.add_conditional_edges("intake", route_risk_assessment, {"pending_approval": "execution", "execute": "execution"})
builder.add_conditional_edges("execution", route_execution_outcome, {"exit": END, "trip_breaker": "circuit_breaker", "retry_loop": "backoff_delay"})
builder.add_edge("backoff_delay", "execution")
builder.add_edge("circuit_breaker", END)

# Compile without the explicit hard stop so it processes end-to-end automatically for our local test
app = builder.compile(checkpointer=MemorySaver())


# 5. RUNTIME VALIDATION

if __name__ == "__main__":
    SESSION_CONFIG = {"configurable": {"thread_id": "production_test_run"}}
    print("\n=================== STARTING AGENT PIPELINE ===================")
    
    # We simulate the manager approving it *after* intake checking by starting with true
    initial_payload = {
        "user_id": "user_456", "action": "issue_refund", "amount": 500,
        "error_count": 0, "result": None, "is_high_risk": True, 
        "is_approved": True, "last_error_code": None, "retry_delay_sec": 0
    }
    
    # Process the entire state machine sequence automatically
    for event in app.stream(initial_payload, config=SESSION_CONFIG):
        pass

    # Fetch final computed state directly from memory checkpoint
    final_system_state = app.get_state(config=SESSION_CONFIG).values
    print(f"\n🏁 [FINAL GRAPH STATE OUTPUT]")
    print(f"-> {final_system_state['result']}")
    print("===============================================================\n")