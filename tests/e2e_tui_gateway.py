import asyncio
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from http.server import ThreadingHTTPServer

from websockets.asyncio.client import connect


REPOSITORY = Path(__file__).resolve().parents[1]
SCRATCH = Path('/Users/levinkin/.hermes/cache/scratch/intercom-desktop/fix4')
RESULT = SCRATCH.parent / 'e2e_result4.txt'


async def host_endpoint(websocket):
    from tui_gateway.ws import handle_ws

    await handle_ws(websocket)


def serve_host():
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute
    from tui_gateway import server
    import uvicorn

    server.install_tui_message_injector()
    application = Starlette(routes=[WebSocketRoute('/api/ws', host_endpoint)])
    listener = socket.socket(fileno=int(sys.argv[2]))
    uvicorn.Server(uvicorn.Config(application, log_level='info', lifespan='off')).run(sockets=[listener])


async def wait_settled(helpers, connection, session_id, marker, canonical=False):
    deadline = time.monotonic() + 60
    snapshot = {}
    while time.monotonic() < deadline:
        method = 'session.resume' if canonical else 'session.activate'
        snapshot = await helpers.rpc(connection, method, session_id=session_id)
        if not snapshot.get('running') and not snapshot.get('pending') and marker in json.dumps(snapshot.get('messages', [])):
            return snapshot
        await asyncio.sleep(.1)
    raise AssertionError(('turn did not settle', session_id, marker, snapshot))


async def invoke(helpers, model, connection, session_id, arguments, expected_ok=True, canonical=False):
    import uuid

    marker = uuid.uuid4().hex
    parameters = {'session_id': session_id, 'text': 'E2E_CALL ' + json.dumps({**arguments, '_marker': marker})}
    if canonical:
        parameters['input_id'] = uuid.uuid4().hex
    await helpers.rpc(connection, 'prompt.submit', **parameters)
    snapshot = await wait_settled(helpers, connection, session_id, marker, canonical=canonical)
    messages = []
    for request in reversed(model.requests):
        if marker not in json.dumps(request['messages']):
            continue
        messages = [message for message in request['messages'] if message.get('role') == 'tool']
        if messages:
            break
    assert messages, snapshot
    result = json.loads(messages[-1]['content'])
    assert result.get('ok') is expected_ok, result
    return result


async def exercise(helpers, home, descriptor, port, model, result):
    grant = helpers.control(home, 'session-ticket', {
        'profile_id': str(home), 'instance_id': descriptor['instance_id'], 'purpose': 'interactive'})
    gateway_url = descriptor['api_origin'].replace('http:', 'ws:') + '/api/ws'
    async with connect(gateway_url, subprotocols=['hermes-gateway-v1', 'hermes-gateway-ticket.' + grant['ticket']]) as gateway:
        async with connect(f'ws://127.0.0.1:{port}/api/ws') as desktop:
            gateway_created = await helpers.rpc(gateway, 'session.create', request_id='fix4-gateway', source='gui',
                                                cwd=str(home), model='intercom-e2e', toolsets=['intercom'], title='fix4-gateway')
            desktop_created = await helpers.rpc(desktop, 'session.create', source='desktop', cwd=str(home),
                                                model='intercom-e2e', title='fix4-desktop')
            gateway_id = gateway_created['session_id']
            desktop_id = desktop_created['session_id']
            result['sessions'] = {'gateway': gateway_id, 'desktop': desktop_id}
            await helpers.rpc(gateway, 'session.resume', session_id=gateway_id)
            await helpers.rpc(desktop, 'session.activate', session_id=desktop_id)
            await invoke(helpers, model, gateway, gateway_id, {'action': 'list'}, canonical=True)
            desktop_list = await invoke(helpers, model, desktop, desktop_id, {'action': 'list'})
            gateway_list = await invoke(helpers, model, gateway, gateway_id, {'action': 'list'}, canonical=True)
            gateway_name = gateway_list['self']['name']
            desktop_name = desktop_list['self']['name']
            assert any(peer['name'] == gateway_name for peer in desktop_list['peers']), desktop_list
            result['checks_passed'] += 1
            assert any(peer['name'] == desktop_name for peer in gateway_list['peers']), gateway_list
            result['checks_passed'] += 1
            result['discovery'] = {'gateway': gateway_list, 'desktop': desktop_list}
            registry = home / 'intercom' / 'sessions'
            peers = [json.loads(path.read_text()) for path in registry.glob('*.json')]
            assert {peer['pid'] for peer in peers} == set(result['pids'].values()), peers
            assert len({peer['socket'] for peer in peers}) == 2, peers
            result['process_peers'] = peers
            result['checks_passed'] += 1
            answered = await invoke(helpers, model, desktop, desktop_id, {
                'action': 'ask', 'to': gateway_name, 'message': 'E2E_QUESTION', 'timeout': 20})
            assert 'E2E_ANSWER' in json.dumps(answered), answered
            replied = await wait_settled(helpers, gateway, gateway_id, 'E2E_QUESTION', canonical=True)
            assert any(message.get('role') == 'tool' and '"ok": true' in message.get('content', '')
                       for message in replied['messages']), replied
            result['ask'] = answered
            result['checks_passed'] += 1
            desktop_peer = next(peer for peer in gateway_list['peers'] if peer['name'] == desktop_name)
            sent = await invoke(helpers, model, gateway, gateway_id, {
                'action': 'send', 'to': desktop_peer['session_id'], 'message': 'E2E_TUI_DELIVERY'}, canonical=True)
            received = await wait_settled(helpers, desktop, desktop_id, 'E2E_TUI_DELIVERY')
            assert any(message.get('role') == 'user' and 'E2E_TUI_DELIVERY' in json.dumps(message)
                       for message in received['messages']), received
            assert any('E2E_TUI_DELIVERY' in json.dumps(request['messages']) for request in model.requests)
            result['send'] = sent
            result['checks_passed'] += 1
            forbidden = await helpers.rpc(desktop, 'session.create', source='discord', cwd=str(home),
                                          model='intercom-e2e', title='fix4-discord')
            refused = await invoke(helpers, model, desktop, forbidden['session_id'], {'action': 'list'}, expected_ok=False)
            assert 'only available to local' in refused.get('error', ''), refused
            result['discord'] = refused
            result['checks_passed'] += 1
            result['model_requests'] = len(model.requests)


def main():
    SCRATCH.mkdir(parents=True, exist_ok=True)
    helpers = None
    home = None
    processes = []
    logs = []
    model = None
    thread = None
    listener = None
    result = {'checks_passed': 0, 'checks_failed': 0, 'pids': {}}
    try:
        previous_temporary = os.environ.get('TMPDIR')
        os.environ['TMPDIR'] = str(SCRATCH)
        try:
            specification = importlib.util.spec_from_file_location('e2e_desktop_helpers', REPOSITORY / 'tests' / 'e2e_desktop.py')
            assert specification is not None and specification.loader is not None
            helpers = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(helpers)
        finally:
            if previous_temporary is None:
                os.environ.pop('TMPDIR', None)
            else:
                os.environ['TMPDIR'] = previous_temporary
        home = Path(tempfile.mkdtemp(prefix='two-host-', dir=SCRATCH))
        (home / 'plugins').mkdir()
        (home / 'plugins' / 'intercom').symlink_to(REPOSITORY, target_is_directory=True)
        (home / 'user').mkdir()
        model = ThreadingHTTPServer(('127.0.0.1', 0), helpers.Model)
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
        environment = {key: os.environ[key] for key in ('PATH', 'LANG', 'TZ') if key in os.environ}
        environment.update(HOME=str(home / 'user'), USERPROFILE=str(home / 'user'), HERMES_HOME=str(home),
                           TMPDIR=str(SCRATCH), PYTHONPATH=str(helpers.CORE), OPENAI_API_KEY='loopback-only',
                           OPENAI_BASE_URL=address, PYTHONUNBUFFERED='1')
        gateway_log = (SCRATCH / 'two_host_gateway.log').open('w')
        logs.append(gateway_log)
        gateway = subprocess.Popen([sys.executable, '-m', 'gateway.run'], cwd=helpers.CORE, env=environment,
                                   stdin=subprocess.DEVNULL, stdout=gateway_log, stderr=subprocess.STDOUT)
        processes.append(gateway)
        result['pids']['gateway'] = gateway.pid
        descriptor = {}
        deadline = time.monotonic() + 90
        while gateway.poll() is None and time.monotonic() < deadline:
            try:
                descriptor = helpers.control(home, 'identify')
                if descriptor.get('state') == 'ready':
                    break
            except (OSError, ValueError):
                pass
            time.sleep(.1)
        assert descriptor.get('state') == 'ready', (SCRATCH / 'two_host_gateway.log').read_text()
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
        desktop_log = (SCRATCH / 'two_host_desktop.log').open('w')
        logs.append(desktop_log)
        desktop = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--host', str(listener.fileno())],
                                   cwd=helpers.CORE, env=environment, pass_fds=(listener.fileno(),),
                                   stdin=subprocess.DEVNULL, stdout=desktop_log, stderr=subprocess.STDOUT)
        processes.append(desktop)
        result['pids']['desktop'] = desktop.pid
        listener.close()
        listener = None
        deadline = time.monotonic() + 60
        ready = False
        while desktop.poll() is None and time.monotonic() < deadline:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    ready = True
                    break
            except OSError:
                time.sleep(.1)
        assert ready, (SCRATCH / 'two_host_desktop.log').read_text()
        result['desktop_port'] = port
        asyncio.run(asyncio.wait_for(exercise(helpers, home, descriptor, port, model, result), timeout=240))
    except BaseException:
        result['checks_failed'] += 1
        result['error'] = traceback.format_exc()
        RESULT.write_text(json.dumps(result, indent=2) + '\n')
    finally:
        try:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                process.wait(timeout=10)
            result['processes_stopped'] = all(process.poll() is not None for process in processes)
            for log in logs:
                log.close()
            if listener is not None:
                listener.close()
            if model is not None:
                model.shutdown()
                model.server_close()
                thread.join(timeout=5)
            if home is not None:
                shutil.rmtree(home)
            if helpers is not None:
                shutil.rmtree(helpers.SCRATCH)
            result['fixtures_removed'] = home is None or not home.exists()
            result['helper_scratch_removed'] = helpers is None or not helpers.SCRATCH.exists()
        except BaseException:
            result['checks_failed'] += 1
            result['cleanup_error'] = traceback.format_exc()
        finally:
            RESULT.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return int(bool(result['checks_failed']))


if __name__ == '__main__':
    if '--host' in sys.argv:
        serve_host()
    else:
        raise SystemExit(main())
