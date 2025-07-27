import pytest
import subprocess

def run_script(commands):
    with subprocess.Popen(
        ["go", "run", "p5/repl.go", "-db", "something.db"],  # Pass the filename!
        stdin=subprocess.PIPE, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, text=True
    ) as proc:
        output, err = proc.communicate(input="\n".join(commands))
        if err:
            raise RuntimeError(f"Script error: {err}")
        return output.split('\n')



def test_insert(n=10, user_id=None, username=None, email=None):
    commands = []
    outputs = ["db > Executed"] * n
    
    for i in range(n):
        user_id_str = str(user_id + i) if user_id is not None else str(i)
        username_str = username + str(user_id) if username is not None else f'user#{i}'
        email_str = email.split('@')[0] + str(user_id) + '@' + email.split('@')[1] if email is not None else f'user{i}@example.com'
        commands.append(f"insert {user_id_str} {username_str} {email_str}")
    return commands, outputs

def test_select(commands):
    outputs = []
    for i, command in enumerate(commands):
        _, userid, username, email = command.split()
        if i == 0:
            outputs.append(f"db > ({userid} {username} {email})")
        else:
            outputs.append(f"({userid} {username} {email})")

    outputs.append("Executed")
    outputs.append("db > ")
    return outputs


def test_match():
    commands, outputs = test_insert(10)
    select_outputs = test_select(commands)
    outputs.extend(select_outputs)
    commands.extend(["select", ".exit"])
    
    results = run_script(commands)
    
    for result, output in zip(results, outputs):
        assert result == output, f"Expected: {output}, but got: {result}"


def test_table_full():
    commands, outputs = test_insert(1401)
    commands.append('.exit')
    outputs[-1] = "db > Error: Table full"
    results = run_script(commands)
    for result, output in zip(results, outputs):
        assert result == output, f"Expected: {output}, but got: {result}"

def test_insert_max_column_size(username_size=64, email_size=512):
    commands, outputs = test_insert(1, 
                        user_id=1, 
                        username='a' * username_size, 
                        email='a' * email_size + '@example.com')
    select_output = test_select(commands)
    outputs.extend(select_output)
    
    commands.extend(["select", ".exit"])
    result = run_script(commands)
    
    for result, output in zip(result, select_output):
        assert result == output, f"Expected: {output}, but got: {result}"

def test_insert_persistence():
    commands, _ = test_insert(10)
    select_outputs = test_select(commands)
    commands.append('.exit')

    _ = run_script(commands)

    select_results = run_script(['select', '.exit'])


    for result, output in zip(select_results, select_outputs):
        assert result == output, f"Expected: {output}, but got: {result}"