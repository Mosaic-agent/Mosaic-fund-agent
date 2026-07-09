from dataclasses import dataclass
from typing import Any
from src.commands.base import Command

@dataclass
class SignalsCommand(Command):
    save: bool
    verbose: bool

    def execute(self) -> dict[str, Any]:
        from src.agents.signal_aggregator import run_signal_aggregation
        report = run_signal_aggregation(save=self.save, verbose=self.verbose)
        return {
            "report": report
        }
