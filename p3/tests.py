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

user_to_email = {
    "alice": "alice@example.com",
    "bob": "bob@example.com",
    "charlie": "charlie@example.com",
    "dave": "dave@example.com",
    "eve": "eve@example.com",
    "frank": "frank@example.com",
    "grace": "grace@example.com",
    "heidi": "heidi@example.com",
    "ivan": "ivan@example.com"
}

def test_insert(n=10):
    commands = []
    outputs = ["Executed\ndb > "] * n
    
    for i in range(n):
        userid = i
        username_index = i % len(user_to_email)
        username = list(user_to_email.keys())[username_index]
        email = user_to_email[username]

        commands.append(f"insert {userid} {username} {email}")
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
    
    # Run the script with the insert commands
    result = run_script(commands + ["select"])
    with pytest.raises(AssertionError):
        assert result == select_output, f"Expected: {select_output}, but got: {result}"


    

