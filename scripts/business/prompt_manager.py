"""
Prompt management system for domain analysis scripts.

This module provides utilities to load, list, and manage prompts stored as JSON files.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Default prompts file (relative to this script)
PROMPTS_FILE = Path(__file__).parent / "prompts.json"


class Prompt:
    """Represents a prompt configuration."""
    
    def __init__(self, data: Dict[str, Any]):
        self.name = data.get("name", "unknown")
        self.description = data.get("description", "")
        self.version = data.get("version", "1.0")
        self.system_prompt = data.get("system_prompt", "")
        self.output_format = data.get("output_format", {})
        self.user_prompt_template = data.get("user_prompt_template", "")
        self._raw_data = data

    @property
    def content_extraction(self) -> Dict[str, Any]:
        raw = self._raw_data.get("content_extraction")
        return raw if isinstance(raw, dict) else {}

    @property
    def content_extraction_mode(self) -> str:
        return str(self.content_extraction.get("mode", "text_only"))

    @property
    def content_extraction_browser_fallback(self) -> bool:
        return bool(self.content_extraction.get("browser_fallback", False))

    @property
    def pass_rules(self) -> Dict[str, Any]:
        raw = self._raw_data.get("pass_rules")
        return raw if isinstance(raw, dict) else {}
    
    def format_user_prompt(self, **kwargs) -> str:
        """Format the user prompt template with provided variables."""
        try:
            return self.user_prompt_template.format(**kwargs)
        except KeyError as e:
            logger.warning(f"Missing template variable {e} in prompt '{self.name}'")
            return self.user_prompt_template
    
    def __repr__(self) -> str:
        return f"Prompt(name='{self.name}', version='{self.version}')"


class PromptManager:
    """Manages loading and accessing prompts from a single JSON file."""
    
    def __init__(self, prompts_file: Optional[Path] = None):
        self.prompts_file = prompts_file or PROMPTS_FILE
        self._prompts: Dict[str, Prompt] = {}
        self._load_prompts()
    
    def _load_prompts(self) -> None:
        """Load all prompts from the prompts.json file."""
        if not self.prompts_file.exists():
            logger.warning(f"Prompts file does not exist: {self.prompts_file}")
            return
        
        try:
            with open(self.prompts_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Check if it's the format with "prompts" array
                if "prompts" in data and isinstance(data["prompts"], list):
                    for prompt_data in data["prompts"]:
                        try:
                            prompt = Prompt(prompt_data)
                            self._prompts[prompt.name] = prompt
                            logger.debug(f"Loaded prompt: {prompt.name} from {self.prompts_file.name}")
                        except Exception as e:
                            logger.error(f"Error creating prompt from {self.prompts_file}: {e}")
                # Legacy format: single prompt object (for custom files)
                elif "name" in data:
                    prompt = Prompt(data)
                    self._prompts[prompt.name] = prompt
                    logger.debug(f"Loaded prompt: {prompt.name} from {self.prompts_file.name}")
                else:
                    logger.warning(f"Unknown format in {self.prompts_file}. Expected 'prompts' array or prompt object.")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON in {self.prompts_file}: {e}")
        except Exception as e:
            logger.error(f"Error loading prompts from {self.prompts_file}: {e}")
    
    def get_prompt(self, name: str) -> Optional[Prompt]:
        """Get a prompt by name."""
        return self._prompts.get(name)
    
    def list_prompts(self) -> List[Prompt]:
        """List all available prompts."""
        return list(self._prompts.values())
    
    def list_prompt_names(self) -> List[str]:
        """List all available prompt names."""
        return sorted(self._prompts.keys())
    
    def load_prompt_from_file(self, file_path: Path, prompt_name: Optional[str] = None) -> Optional[Prompt]:
        """Load a prompt from a specific file.
        
        Args:
            file_path: Path to the JSON file
            prompt_name: If the file contains multiple prompts, specify which one to load
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Check if it's the new format with "prompts" array
                if "prompts" in data and isinstance(data["prompts"], list):
                    if prompt_name:
                        # Find the specific prompt by name
                        for prompt_data in data["prompts"]:
                            if prompt_data.get("name") == prompt_name:
                                return Prompt(prompt_data)
                        logger.warning(f"Prompt '{prompt_name}' not found in {file_path}")
                        return None
                    else:
                        # Return the first prompt if no name specified
                        if data["prompts"]:
                            return Prompt(data["prompts"][0])
                        return None
                # Legacy format: single prompt object
                elif "name" in data:
                    return Prompt(data)
                else:
                    logger.error(f"Unknown format in {file_path}")
                    return None
        except Exception as e:
            logger.error(f"Error loading prompt from {file_path}: {e}")
            return None
    
    def reload(self) -> None:
        """Reload all prompts from disk."""
        self._prompts.clear()
        self._load_prompts()


# Global prompt manager instance
_prompt_manager: Optional[PromptManager] = None


def get_prompt_manager(prompts_file: Optional[Path] = None) -> PromptManager:
    """Get or create the global prompt manager instance."""
    global _prompt_manager
    if _prompt_manager is None:
        _prompt_manager = PromptManager(prompts_file)
    return _prompt_manager


def get_prompt(name: str, prompts_file: Optional[Path] = None) -> Optional[Prompt]:
    """Get a prompt by name."""
    manager = get_prompt_manager(prompts_file)
    return manager.get_prompt(name)


def list_prompts(prompts_file: Optional[Path] = None) -> List[Prompt]:
    """List all available prompts."""
    manager = get_prompt_manager(prompts_file)
    return manager.list_prompts()


def load_prompt_from_file(file_path: Path, prompt_name: Optional[str] = None) -> Optional[Prompt]:
    """Load a prompt from a specific file.
    
    Args:
        file_path: Path to the JSON file
        prompt_name: If the file contains multiple prompts, specify which one to load
    """
    manager = get_prompt_manager()
    return manager.load_prompt_from_file(file_path, prompt_name)

