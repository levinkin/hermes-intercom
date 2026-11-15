# hermes-intercom

lets your hermes chats on one machine find each other and message each other. works across desktop app chats, `hermes chat` and the tui.

based on the proposal in [NousResearch/hermes-agent#81885](https://github.com/NousResearch/hermes-agent/issues/81885) and forked from [majordave/hermes-intercom](https://github.com/majordave/hermes-intercom), which covered cli/tui only. this version runs inside the gateway and the desktop app's backend, so desktop chats work too.

## install

```bash
hermes plugins install levinkin/hermes-intercom
hermes config set plugins.entries.intercom.allow_gateway_injection true
hermes gateway restart
```

restart the desktop app's backend too. it keeps running after you quit the app, so stop it or reboot.

new chats get the tool. ask one:

> use intercom to list sessions, then ask the other chat what 2+2 is

## the tool

one tool, `intercom`, with an `action`:

| action | params | what it does |
|---|---|---|
| `list` | none | live chats (name, cwd, busy/idle) |
| `send` | `to` (string), `message` (string, max 8000 chars) | delivers a message, returns `delivered` or `refused` |
| `ask` | `to`, `message`, `timeout` (integer, default `600`, max `3600`) | sends and waits for the reply |
| `reply` | `request_id` (string), `message` | answers an `ask` |

names are the chat titles. two chats with the same title get an id suffix.

an idle chat starts a turn when a message comes in. a busy one gets it at its next turn. messages show up as:

```
📡 hermes@<chat name> says:
<message>
```

messages are inert text. they can't approve actions or run slash commands.

## settings

| key | type | accepts | default |
|---|---|---|---|
| `plugins.entries.intercom.allow_gateway_injection` | bool | `true`/`false` | `false` (needs `true`) |
| `plugins.entries.intercom.settings.inbound` | string | `always`, `never` | `always` (`never` = this profile can send but not receive) |

## security

- only local chats (desktop/cli/tui) can use it. discord, telegram and other messaging sessions get refused.
- chats in the same process talk in memory. nothing goes over a socket.
- between processes (gateway and desktop backend) messages go over a `0600` unix socket and the receiver checks the peer's uid.
- every message between processes is sealed with aes-256-gcm. sender, recipient, type, request id and timestamp are bound in, so a message can't be moved to another chat or replayed (120 s window, nonces tracked).
- the key lives at `~/.hermes/intercom/key` (`0600`). anything running as your user can read it, same as your ssh keys.
- full queues and dead chats return `refused`, never a fake `delivered`. rate limit is 6/min per sender per chat.

## tests

```bash
python tests/test_protocol.py
python tests/e2e_desktop.py
python tests/e2e_tui_gateway.py
```

use the gateway's python. the e2e runs start a throwaway gateway and desktop host on spare ports with a stub model (no tokens).

## license

mit. original code by majordave, desktop/gateway rewrite by kevin lin.
