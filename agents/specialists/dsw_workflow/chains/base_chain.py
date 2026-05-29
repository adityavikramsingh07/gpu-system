from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import SystemMessage, HumanMessage

def invoke_gemini(system_prompt: str, user_prompt: str) -> str:
    llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)
    res = llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)])
    return res.content
