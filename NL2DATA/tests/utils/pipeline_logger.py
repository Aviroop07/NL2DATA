"""Pipeline logger utility for capturing coherent LLM request/response pairs.

Standard rule for this project:
- Log entries must be request+response pairs in a single, coherent block.
- Avoid extra sections (tool calls, partial outputs, schemas) in the pipeline log.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
from threading import Lock

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def _serialize_messages_for_api(messages_sent: List[Any]) -> List[Dict[str, Any]]:
    """
    Convert LangChain messages to a stable, OpenAI-like wire format:
    [{"role": "system"|"user"|"assistant"|"tool", "content": "..."}].

    We intentionally omit tool_calls/tool_call_id to keep the pipeline log minimal and coherent.
    """
    serialized: List[Dict[str, Any]] = []
    for msg in messages_sent:
        if hasattr(msg, "dict"):
            msg_dict = msg.dict()
            msg_type = (msg_dict.get("type", "") or "").replace("message", "").lower()
            role = "user"
            if msg_type in {"system"}:
                role = "system"
            elif msg_type in {"human"}:
                role = "user"
            elif msg_type in {"ai", "assistant"}:
                role = "assistant"
            elif msg_type in {"tool"}:
                role = "tool"
            serialized.append({"role": role, "content": msg_dict.get("content", "")})
        else:
            # Best-effort fallback
            serialized.append({"role": "user", "content": str(msg)})
    return serialized


class PipelineLogger:
    """Logger for capturing pipeline intermediate outputs."""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PipelineLogger, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.output_file: Optional[Path] = None
        self.file_handle = None
        self._initialized = True
    
    def initialize(self, output_dir: str, filename: str = "pipeline_output.txt"):
        """
        Initialize the logger with output directory and filename.
        
        Args:
            output_dir: Directory path where output file will be created
            filename: Name of the output file
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self.output_file = output_path / filename
        
        # Open file in append mode (will create if doesn't exist)
        self.file_handle = open(self.output_file, 'w', encoding='utf-8')
        
        # Write header
        self._write_separator()
        self._write_line("PIPELINE EXECUTION LOG")
        self._write_line(f"Started at: {datetime.now().isoformat()}")
        self._write_separator()
        self.file_handle.flush()
        # Intentionally avoid emitting INFO logs from the pipeline logger itself.
    
    def _write_separator(self, char: str = "=", length: int = 80):
        """Write a separator line."""
        if self.file_handle:
            self.file_handle.write(char * length + "\n")
    
    def _write_line(self, text: str = ""):
        """Write a line to the file."""
        if self.file_handle:
            self.file_handle.write(text + "\n")
    
    def log_llm_call(
        self,
        step_name: str,
        prompt: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        response: Optional[Any] = None,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        tool_results: Optional[List[Dict[str, Any]]] = None,
        messages_sent: Optional[List[Any]] = None,
        raw_response: Optional[Any] = None,
        attempt_number: Optional[int] = None,
        llm_params: Optional[Dict[str, Any]] = None,
    ):
        """Log a single coherent REQUEST/RESPONSE pair.

        Notes:
        - We intentionally do not log tool calls/results, raw response metadata, schemas, etc.
        - We use ASCII-safe JSON (`ensure_ascii=True`) to avoid Windows console/log issues.
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"LLM_CALL: {step_name}")
        if attempt_number:
            self._write_line(f"ATTEMPT: {attempt_number}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        # REQUEST
        if messages_sent:
            payload = _serialize_messages_for_api(messages_sent)
            self._write_line("REQUEST:")
            self._write_line(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
        elif prompt:
            self._write_line("REQUEST:")
            self._write_line(str(prompt))
        elif input_data is not None:
            self._write_line("REQUEST:")
            self._write_line(json.dumps(input_data, indent=2, ensure_ascii=True, default=str))
        else:
            self._write_line("REQUEST:")
            self._write_line("(unavailable)")

        # RESPONSE (prefer parsed/final)
        self._write_line("")
        self._write_line("RESPONSE:")
        if response is None and raw_response is not None:
            response = raw_response

        try:
            if hasattr(response, "model_dump"):
                self._write_line(json.dumps(response.model_dump(), indent=2, ensure_ascii=True, default=str))
            elif isinstance(response, dict):
                self._write_line(json.dumps(response, indent=2, ensure_ascii=True, default=str))
            elif hasattr(response, "content"):
                self._write_line(str(getattr(response, "content", "")))
            else:
                self._write_line(str(response))
        except Exception as e:
            self._write_line(f"(failed to serialize response: {e})")
            self._write_line(str(response))
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
    # NOTE: We intentionally do not provide schema/ER-diagram logging here anymore.
    # If needed later, implement a separate artifact writer that writes to files, not logs.
    
    def log_intermediate_output(self, step_name: str, output: Any, output_type: str = "INTERMEDIATE OUTPUT"):
        """
        Log any intermediate output from the pipeline.
        
        Args:
            step_name: Name of the step
            output: The output to log (can be dict, Pydantic model, or any serializable object)
            output_type: Type label for the output
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"{output_type}: {step_name}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        try:
            if hasattr(output, 'model_dump'):
                output_dict = output.model_dump()
                self._write_line(json.dumps(output_dict, indent=2, default=str))
            elif isinstance(output, dict):
                self._write_line(json.dumps(output, indent=2, default=str))
            else:
                self._write_line(str(output))
        except Exception as e:
            self._write_line(f"Error serializing output: {e}")
            self._write_line(str(output))
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
    def log_error(self, step_name: str, errors: List[Dict[str, Any]]):
        """
        Log errors that occurred during execution.
        
        Args:
            step_name: Name of the step where error occurred
            errors: List of error dictionaries with keys like 'error', 'tool', 'type', etc.
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"ERROR: {step_name}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        try:
            self._write_line("=== ERRORS ===")
            for i, error in enumerate(errors, 1):
                self._write_line(f"\nError {i}:")
                self._write_line(json.dumps(error, indent=2, default=str))
            self._write_line("=== END ERRORS ===")
        except Exception as e:
            self._write_line(f"Error serializing errors: {e}")
            self._write_line(str(errors))
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
    def close(self):
        """Close the log file."""
        if self.file_handle:
            self._write_separator()
            self._write_line(f"Pipeline execution ended at: {datetime.now().isoformat()}")
            self._write_separator()
            self.file_handle.close()
            self.file_handle = None
            logger.info(f"Pipeline logger closed: {self.output_file}")


# Global instance
_pipeline_logger = PipelineLogger()


def get_pipeline_logger() -> PipelineLogger:
    """Get the global pipeline logger instance."""
    return _pipeline_logger

