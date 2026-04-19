import pytest
from datetime import datetime
from src.commands.base import Command, CommandRunner, CommandRecord

class SuccessCommand(Command):
    def __init__(self):
        self.executed = False
        self.undone = False

    def execute(self):
        self.executed = True
        return "success"

    def undo(self):
        self.undone = True

class FailureCommand(Command):
    def execute(self):
        raise ValueError("failed")

def test_runner_records_history():
    runner = CommandRunner()
    cmd = SuccessCommand()
    result = runner.run(cmd)
    
    assert result == "success"
    assert len(runner.history) == 1
    assert runner.history[0].success is True
    assert runner.history[0].result == "success"
    assert cmd.executed is True

def test_runner_retry_exhausts_attempts():
    runner = CommandRunner(max_retries=3)
    cmd = FailureCommand()
    
    with pytest.raises(ValueError, match="failed"):
        runner.run(cmd, retryable=True)
    
    assert len(runner.history) == 1
    assert runner.history[0].success is False
    assert runner.history[0].error == "failed"

def test_undo_calls_correct_command():
    runner = CommandRunner()
    cmd1 = SuccessCommand()
    cmd2 = SuccessCommand()
    
    runner.run(cmd1)
    runner.run(cmd2)
    
    runner.undo_last()
    assert cmd2.undone is True
    assert cmd1.undone is False

def test_undo_throws_if_nothing_to_undo():
    runner = CommandRunner()
    with pytest.raises(RuntimeError, match="Nothing to undo"):
        runner.undo_last()
