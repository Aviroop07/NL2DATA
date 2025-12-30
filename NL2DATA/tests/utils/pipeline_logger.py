"""Pipeline logger utility for capturing LLM responses, tool calls, ER diagrams, and schemas.

This module provides a centralized logging mechanism that writes all pipeline
intermediate outputs to a text file for debugging and analysis.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
from threading import Lock

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


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
        
        logger.info(f"Pipeline logger initialized: {self.output_file}")
    
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
        """
        Log an LLM call with its prompt, response, and tool calls.
        
        Args:
            step_name: Name of the step (e.g., "Phase 1.1: Domain Detection")
            prompt: The prompt sent to the LLM (legacy, for backward compatibility)
            input_data: Input data dictionary
            response: The LLM response (can be Pydantic model or dict) - parsed/final response
            tool_calls: List of tool calls made by the LLM
            tool_results: List of tool call results
            messages_sent: List of actual LangChain messages sent to LLM (SystemMessage, HumanMessage, etc.)
            raw_response: Raw response from LLM before parsing (AIMessage object)
            attempt_number: Retry attempt number if this is a retry
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"LLM CALL: {step_name}")
        if attempt_number:
            self._write_line(f"ATTEMPT: {attempt_number}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        # Only log serialized message list (as sent to API)
        if messages_sent:
            try:
                serialized_messages = []
                for msg in messages_sent:
                    # Convert message to the exact format sent to OpenAI API
                    if hasattr(msg, 'dict'):
                        msg_dict = msg.dict()
                        # Extract only the fields that go to the API
                        api_msg = {
                            "role": msg_dict.get("type", "").replace("message", "").lower() or "user",
                            "content": msg_dict.get("content", ""),
                        }
                        # Fix role names to match OpenAI API format
                        if api_msg["role"] == "system":
                            api_msg["role"] = "system"
                        elif api_msg["role"] == "human":
                            api_msg["role"] = "user"
                        elif api_msg["role"] == "ai":
                            api_msg["role"] = "assistant"
                        elif api_msg["role"] == "tool":
                            api_msg["role"] = "tool"
                        
                        # Add tool_calls if present
                        if msg_dict.get("tool_calls"):
                            api_msg["tool_calls"] = msg_dict["tool_calls"]
                        
                        # Add tool_call_id if present (for tool messages)
                        if msg_dict.get("tool_call_id"):
                            api_msg["tool_call_id"] = msg_dict["tool_call_id"]
                        
                        serialized_messages.append(api_msg)
                
                self._write_line("--- Serialized Message List (as sent to API) ---")
                self._write_line(json.dumps(serialized_messages, indent=2, ensure_ascii=False))
                self._write_line("--- End Serialized Message List ---")
            except Exception as e:
                self._write_line(f"Error serializing messages to API format: {e}")
        
        # Log tool calls if available
        if tool_calls:
            self._write_line("\n=== TOOL CALLS ===")
            try:
                for i, tool_call in enumerate(tool_calls, 1):
                    self._write_line(f"\nTool Call {i}:")
                    self._write_line(json.dumps(tool_call, indent=2, default=str))
            except Exception as e:
                self._write_line(f"Error serializing tool calls: {e}")
                self._write_line(str(tool_calls))
            self._write_line("=== END TOOL CALLS ===\n")
        
        # Log tool results if available
        if tool_results:
            self._write_line("\n=== TOOL RESULTS ===")
            try:
                for i, tool_result in enumerate(tool_results, 1):
                    self._write_line(f"\nTool Result {i}:")
                    self._write_line(json.dumps(tool_result, indent=2, default=str))
            except Exception as e:
                self._write_line(f"Error serializing tool results: {e}")
                self._write_line(str(tool_results))
            self._write_line("=== END TOOL RESULTS ===\n")
        
        # Only log parsed/final response
        if response is not None:
            self._write_line("\n=== PARSED/FINAL RESPONSE ===")
            try:
                # Handle Pydantic models
                if hasattr(response, 'model_dump'):
                    response_dict = response.model_dump()
                    self._write_line(json.dumps(response_dict, indent=2, default=str))
                elif isinstance(response, dict):
                    self._write_line(json.dumps(response, indent=2, default=str))
                else:
                    self._write_line(str(response))
            except Exception as e:
                self._write_line(f"Error serializing response: {e}")
                self._write_line(str(response))
            self._write_line("=== END PARSED RESPONSE ===\n")
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
    def log_er_diagram(self, step_name: str, er_design: Dict[str, Any]):
        """
        Log an ER diagram/schema.
        
        Args:
            step_name: Name of the step (e.g., "Phase 3.4: ER Design Compilation")
            er_design: ER design dictionary
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"ER DIAGRAM: {step_name}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        try:
            self._write_line(json.dumps(er_design, indent=2, default=str))
        except Exception as e:
            self._write_line(f"Error serializing ER design: {e}")
            self._write_line(str(er_design))
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
    def log_schema(self, step_name: str, schema: Dict[str, Any]):
        """
        Log a relational schema.
        
        Args:
            step_name: Name of the step (e.g., "Phase 3.5: Relational Schema Compilation")
            schema: Schema dictionary
        """
        if not self.file_handle:
            return
        
        self._write_separator()
        self._write_line(f"RELATIONAL SCHEMA: {step_name}")
        self._write_line(f"Timestamp: {datetime.now().isoformat()}")
        self._write_separator("-")
        
        try:
            self._write_line(json.dumps(schema, indent=2, default=str))
        except Exception as e:
            self._write_line(f"Error serializing schema: {e}")
            self._write_line(str(schema))
        
        self._write_separator()
        self._write_line()
        self.file_handle.flush()
    
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

