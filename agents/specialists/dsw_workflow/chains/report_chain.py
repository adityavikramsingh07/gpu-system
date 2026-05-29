import json
from .base_chain import invoke_gemini

def run_report_chain(system_prompt: str, analysis: str, correlation: str, recommendation: str) -> dict:
    prompt = (
        "Synthesize the preceding phases into a structured JSON payload.\n\n"
        f"Analysis:\n{analysis}\n\n"
        f"Correlation:\n{correlation}\n\n"
        f"Recommendations:\n{recommendation}\n\n"
        "Output EXACTLY this JSON format (no markdown blocks):\n"
        "{\n"
        '  "summary": "High-level summary of the entire analysis",\n'
        '  "data": { "key": "value" },\n'
        '  "confidence": 0.95\n'
        "}"
    )
    content = invoke_gemini(system_prompt, prompt).strip()
    if content.startswith("```json"): content = content[7:]
    if content.startswith("```"): content = content[3:]
    if content.endswith("```"): content = content[:-3]
    try:
        return json.loads(content.strip())
    except json.JSONDecodeError:
        return {"summary": "Failed to parse structured output.", "data": {"raw": content}, "confidence": 0.5}
