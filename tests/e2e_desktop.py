import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from functools import partial
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from types import ModuleType

from websockets.asyncio.client import connect


CORE = Path(sys.executable).absolute().parents[2] / 'workspace'
if not (CORE / 'gateway' / 'run.py').is_file():
    specification = importlib.util.find_spec('hermes_cli')
    assert specification is not None and specification.origin is not None, 'Hermes core is not installed'
    CORE = Path(specification.origin).resolve().parents[1]
SCRATCH = Path(tempfile.mkdtemp(prefix='intercom-desktop-', dir=os.environ.get('TMPDIR'))).resolve()
REPOSITORY = Path(__file__).resolve().parents[1]


class Model(BaseHTTPRequestHandler):
    def log_message(self, *arguments):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append(body)
        messages = body.get('messages', [])
        latest = next((message for message in reversed(messages) if message['role'] == 'user'), {})
        text = latest.get('content', '')
        if not isinstance(text, str):
            text = json.dumps(text)
        last = messages[-1] if messages else {}
        arguments = None
        if last.get('role') != 'tool':
            if text.startswith('E2E_CALL '):
                arguments = json.loads(text.removeprefix('E2E_CALL '))
            elif 'E2E_QUESTION' in text and 'request_id=' in text:
                request_id = re.search(r'request_id="([^"]+)"', text)
                target = re.search(r'to="([^"]+)"', text)
                assert request_id and target, text
                arguments = {'action': 'reply', 'to': target[1], 'request_id': request_id[1], 'message': 'E2E_ANSWER'}
        message = {'role': 'assistant', 'content': 'E2E_DONE'}
        reason = 'stop'
        if arguments:
            available = {tool.get('function', {}).get('name') for tool in body.get('tools', [])}
            function = 'intercom'
            if function not in available:
                assert 'tool_call' in available, available
                function = 'tool_call'
                arguments = {'calls': [{'name': 'intercom', 'arguments': arguments}]}
            message = {'role': 'assistant', 'content': None, 'tool_calls': [{
                'id': 'call_' + uuid.uuid4().hex, 'type': 'function',
                'function': {'name': function, 'arguments': json.dumps(arguments)}}]}
            reason = 'tool_calls'
        response = {'id': 'local-e2e', 'object': 'chat.completion', 'model': 'intercom-e2e',
                    'choices': [{'index': 0, 'message': message, 'finish_reason': reason}],
                    'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}
        payload = json.dumps(response).encode()
        kind = 'application/json'
        if body.get('stream'):
            payload = ('data: ' + json.dumps({'id': 'local-e2e', 'choices': [
                {'index': 0, 'delta': message, 'finish_reason': reason}]}) + '\n\ndata: [DONE]\n\n').encode()
            kind = 'text/event-stream'
        self.send_response(200)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def control(home, verb, parameters=None):
    pointer = home / 'gateway.sock.path'
    path = pointer.read_text().strip() if pointer.exists() else str(home / 'gateway.sock')
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(2)
        connection.connect(path)
        connection.sendall(json.dumps({'protocol': 1, 'id': 1, 'verb': verb, 'params': parameters or {}}).encode() + b'\n')
        with connection.makefile('rb') as stream:
            reply = json.loads(stream.readline())
    assert reply['ok'], reply
    return reply['result']


async def rpc(connection, method, **parameters):
    identifier = uuid.uuid4().hex
    await connection.send(json.dumps({'jsonrpc': '2.0', 'id': identifier, 'method': method, 'params': parameters}))
    async with asyncio.timeout(20):
        while True:
            reply = json.loads(await connection.recv())
            if reply.get('id') == identifier:
                assert 'result' in reply, reply
                return reply['result']


async def settled(connection, session_id, marker):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        snapshot = await rpc(connection, 'session.resume', session_id=session_id)
        if not snapshot['running'] and not snapshot['pending'] and marker in '\n'.join(str(message.get('content', '')) for message in snapshot['messages']):
            return snapshot
        await asyncio.sleep(.1)
    raise AssertionError(('turn did not settle', session_id, marker, snapshot))


async def call(connection, session_id, arguments, expected_ok=True):
    identifier = uuid.uuid4().hex
    await rpc(connection, 'prompt.submit', session_id=session_id, input_id=identifier,
              text='E2E_CALL ' + json.dumps(arguments))
    snapshot = await settled(connection, session_id, json.dumps(arguments))
    results = [message for message in snapshot['messages'] if message.get('role') == 'tool']
    assert results, snapshot
    result = json.loads(results[-1]['content'])
    assert result.get('ok') is expected_ok, result
    return result


def load_inbox(home):
    constants = ModuleType('hermes_constants')
    constants.get_hermes_home = partial(str, home)
    sys.modules['hermes_constants'] = constants
    package = ModuleType('intercom_e2e')
    package.__path__ = [str(REPOSITORY)]
    sys.modules[package.__name__] = package
    specification = importlib.util.spec_from_file_location('intercom_e2e.inbox', REPOSITORY / 'inbox.py')
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    module._initialize_key()
    return module


def exchange_envelope(inbox, peer, envelope):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(peer['socket'])
        inbox._send_frame(connection, envelope)
        try:
            response = inbox._read_frame(connection)
        except (EOFError, ConnectionResetError):
            return None
    assert response['sender'] in (peer['session_id'], peer['session_key']), response
    assert response['destination'] == envelope['sender'], response
    return inbox._open(response, destination=envelope['sender'])


async def exercise(home, descriptor, model):
    grant = control(home, 'session-ticket', {'profile_id': str(home), 'instance_id': descriptor['instance_id'], 'purpose': 'interactive'})
    url = descriptor['api_origin'].replace('http:', 'ws:') + '/api/ws'
    async with connect(url, subprotocols=['hermes-gateway-v1', 'hermes-gateway-ticket.' + grant['ticket']]) as connection:
        sessions = []
        for name in ('e2e-alpha', 'e2e-beta'):
            created = await rpc(connection, 'session.create', request_id=name, source='gui',
                                cwd=str(home), model='intercom-e2e', toolsets=['intercom'], title=name)
            session_id = created['session_id']
            sessions.append(session_id)
            await rpc(connection, 'session.resume', session_id=session_id)
        await call(connection, sessions[1], {'action': 'list'})
        listed = await call(connection, sessions[0], {'action': 'list'})
        peers = listed['peers']
        assert len(peers) == 1, listed
        target = peers[0]['name']
        assert target != listed['self']['name'], listed
        sent = await call(connection, sessions[0], {'action': 'send', 'to': target, 'message': 'E2E_DELIVERY'})
        received = await settled(connection, sessions[1], 'E2E_DELIVERY')
        inbound = [message for message in received['messages'] if message.get('role') == 'user' and 'E2E_DELIVERY' in json.dumps(message)]
        assert inbound and 'hermes@' in json.dumps(inbound) and 'says:' in json.dumps(inbound), received
        assert any('E2E_DELIVERY' in json.dumps(request['messages']) for request in model.requests), model.requests
        answered = await call(connection, sessions[0], {'action': 'ask', 'to': target, 'message': 'E2E_QUESTION', 'timeout': 20})
        assert 'E2E_ANSWER' in json.dumps(answered), answered
        reply = await settled(connection, sessions[1], 'E2E_QUESTION')
        assert any(message.get('role') == 'tool' and '"ok": true' in message.get('content', '') for message in reply['messages']), reply
        inbox = load_inbox(home)
        registry = home / 'intercom' / 'sessions'
        peer = next(json.loads(path.read_text()) for path in registry.glob('*.json')
                    if json.loads(path.read_text())['name'] == target)
        assert stat.S_IMODE((home / 'intercom' / 'key').stat().st_mode) == 0o600
        assert stat.S_IMODE((home / 'intercom').stat().st_mode) == 0o700
        assert stat.S_IMODE(registry.stat().st_mode) == 0o700
        assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in registry.glob('*.json'))
        sender = uuid.uuid4().hex
        payload = {'type': 'send', 'from_name': 'e2e-security', 'from_cwd': str(home),
                   'session_key': peer['session_key'], 'message': 'E2E_FORGED'}
        key = inbox._state['key']
        try:
            inbox._state['key'] = os.urandom(32)
            forged = inbox._seal(payload, sender, peer['session_id'])
        finally:
            inbox._state['key'] = key
        rejected = await asyncio.to_thread(exchange_envelope, inbox, peer, forged)
        assert rejected is None or rejected.get('ok') is False, rejected
        payload['message'] = 'E2E_REPLAY'
        envelope = inbox._seal(payload, sender, peer['session_id'])
        accepted = await asyncio.to_thread(exchange_envelope, inbox, peer, envelope)
        assert accepted and accepted.get('ok') is True, accepted
        replayed = await asyncio.to_thread(exchange_envelope, inbox, peer, envelope)
        assert replayed is None or replayed.get('ok') is False, replayed
        secured = await settled(connection, sessions[1], 'E2E_REPLAY')
        incoming = [message for message in secured['messages'] if message.get('role') == 'user']
        assert sum('E2E_REPLAY' in json.dumps(message) for message in incoming) == 1, incoming
        assert not any('E2E_FORGED' in json.dumps(message) for message in incoming), incoming
        unavailable_id = uuid.uuid4().hex
        unavailable_path = registry / f'{unavailable_id}.json'
        inbox._write_registry(unavailable_path, {
            'name': 'e2e-unavailable', 'pid': os.getpid(), 'cwd': str(home),
            'session_id': unavailable_id, 'socket': str(SCRATCH / f'{unavailable_id}.sock')})
        try:
            unavailable = await call(connection, sessions[0], {
                'action': 'send', 'to': 'e2e-unavailable', 'message': 'E2E_UNAVAILABLE'}, expected_ok=False)
            assert not unavailable_path.exists(), unavailable
        finally:
            unavailable_path.unlink(missing_ok=True)
        grant = control(home, 'session-ticket', {'profile_id': str(home), 'instance_id': descriptor['instance_id'], 'purpose': 'interactive'})
        async with connect(url, subprotocols=['hermes-gateway-v1', 'hermes-gateway-ticket.' + grant['ticket']]) as cli_connection:
            created = await rpc(cli_connection, 'session.create', request_id='e2e-cli', source='cli',
                                cwd=str(home), row_cwd=str(home), model='intercom-e2e',
                                toolsets=['intercom'], title='e2e-cli')
            cli_session_id = created['session_id']
            await rpc(cli_connection, 'session.resume', session_id=cli_session_id)
            cli_listed = await call(cli_connection, cli_session_id, {'action': 'list'})
            assert any(peer['name'] == target for peer in cli_listed['peers']), cli_listed
            cli_sent = await call(cli_connection, cli_session_id, {
                'action': 'send', 'to': target, 'message': 'E2E_CLI_DELIVERY'})
            cli_received = await settled(connection, sessions[1], 'E2E_CLI_DELIVERY')
            assert any(message.get('role') == 'user' and 'E2E_CLI_DELIVERY' in json.dumps(message)
                       for message in cli_received['messages']), cli_received
            assert any('E2E_CLI_DELIVERY' in json.dumps(request['messages']) for request in model.requests), model.requests
        return {'checks_passed': 9, 'checks_failed': 0, 'original_checks_passed': 4,
                'security_checks_passed': 4, 'cli_checks_passed': 1,
                'cli_session': cli_session_id, 'cli_list': cli_listed, 'cli_send': cli_sent,
                'key_mode': '0600', 'wrong_key': rejected,
                'replay': replayed, 'unavailable': unavailable, 'sessions': sessions,
                'list': listed, 'send': sent, 'ask': answered,
                'model_requests': len(model.requests), 'api_origin': descriptor['api_origin']}


def main():
    SCRATCH.mkdir(parents=True, exist_ok=True)
    home = Path(tempfile.mkdtemp(prefix='e2e-', dir=SCRATCH))
    process = None
    model = None
    log = None
    result = {'checks_passed': 0, 'checks_failed': 1}
    try:
        (home / 'plugins').mkdir()
        (home / 'plugins' / 'intercom').symlink_to(REPOSITORY, target_is_directory=True)
        (home / 'user').mkdir()
        model = ThreadingHTTPServer(('127.0.0.1', 0), Model)
        model.requests = []
        thread = threading.Thread(target=model.serve_forever, daemon=True)
        thread.start()
        address = f'http://127.0.0.1:{model.server_port}/v1'
        configuration = {'_config_version': 12, 'gateway': {'multiplex_profiles': False},
                         'model': {'provider': 'custom', 'default': 'intercom-e2e', 'base_url': address, 'context_length': 128000},
                         'plugins': {'enabled': ['intercom'], 'entries': {'intercom': {'allow_gateway_injection': True}}},
                         'auxiliary': {'title_generation': {'enabled': False}}}
        (home / 'config.yaml').write_text(json.dumps(configuration))
        (home / 'models_dev_cache.json').write_text(json.dumps({'e2e-offline': {'id': 'e2e-offline', 'env': [], 'models': {}}}))
        environment = {key: os.environ[key] for key in ('PATH', 'LANG', 'TZ', 'TMPDIR') if key in os.environ}
        environment.update(HOME=str(home / 'user'), USERPROFILE=str(home / 'user'), HERMES_HOME=str(home),
                           PYTHONPATH=str(CORE), OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL=address, PYTHONUNBUFFERED='1')
        log = (SCRATCH / 'e2e_gateway.log').open('w')
        process = subprocess.Popen([sys.executable, '-m', 'gateway.run'], cwd=CORE,
                                   env=environment, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 90
        descriptor = {}
        while process.poll() is None and time.monotonic() < deadline:
            try:
                descriptor = control(home, 'identify')
                if descriptor.get('state') == 'ready':
                    break
            except (OSError, ValueError):
                pass
            time.sleep(.1)
        assert descriptor.get('state') == 'ready', (SCRATCH / 'e2e_gateway.log').read_text()
        result = asyncio.run(asyncio.wait_for(exercise(home, descriptor, model), timeout=150))
        result['gateway_pid'] = process.pid
    except Exception:
        result['error'] = traceback.format_exc()
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
            process.wait(timeout=10)
            result['gateway_stopped'] = process.poll() is not None
        if log is not None:
            log.close()
        if model is not None:
            model.shutdown()
            model.server_close()
            thread.join(timeout=5)
        shutil.rmtree(home)
        result['scratch_home_removed'] = not home.exists()
        (SCRATCH / 'e2e_result.txt').write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return int(bool(result['checks_failed']))


if __name__ == '__main__':
    sys.exit(main())
