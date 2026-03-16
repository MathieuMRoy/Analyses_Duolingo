import asyncio
from google.adk.agents import LlmAgent
from google.adk.runners import InMemoryRunner
import google.genai.types as genai_types

async def test():
    agent_def = LlmAgent(name="test", model="gemini-2.0-flash")
    runner = InMemoryRunner(agent=agent_def)
    msg = genai_types.Content(role="user", parts=[genai_types.Part.from_text(text="say hello")])
    
    # Session must be created before runner.run can use it
    await runner.memory_service.create_session("s")
    
    for event in runner.run(user_id="u", session_id="s", new_message=msg):
        print("EVENT:", type(event))
        print("ACTIONS:", hasattr(event, "actions"))
        if hasattr(event, "actions") and event.actions:
            for action in event.actions:
                print("  ACTION:", type(action), getattr(action, "message", "no_message"))

if __name__ == "__main__":
    asyncio.run(test())
