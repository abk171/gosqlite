import pytest
import subprocess

def run_script(commands):
    with subprocess.Popen(
        ["go", "run", "repl.go"], 
        stdin=subprocess.PIPE, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, text=True
    ) as proc:
        for command in commands:
            proc.stdin.write(command + "\n")
        proc.wait(1)
        output, _ = proc.communicate()
        return output.split('\n')



def test_insert(n=10):
    commands = []
    outputs = ["Executed\ndb > "] * n
    
    for i in range(n):
        commands.append(f"insert {i} user#{i} user{i}@example.com")
    return commands, outputs

def test_select(commands):
    output = []
    for command in commands:
        _, userid, username, email = command.split()
        output.append(f"({userid} {username} {email})")

    output.append("\nExecuted\ndb > ")
    return output


def test_match():
    commands, outputs = test_insert(10)
    select_output = test_select(commands)
    result = run_script(commands + ["select"])

    with pytest.raises(AssertionError):
        assert result == select_output, f"Expected: {select_output}, but got: {result}"


def test_table_full():
    commands, outputs = test_insert(1401)
    outputs[-1] = "Error: Table full\ndb > "
    result = run_script(commands)

    with pytest.raises(AssertionError):
        assert result == outputs, f"Expected: {outputs}, but got: {result}"

