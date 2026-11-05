import json
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, 'deepseek-harness/scripts')
from matrix_channel import MatrixClient, matrix_events  # noqa: E402

url = 'http://127.0.0.1:6167'
for _ in range(60):
    try:
        with urllib.request.urlopen(url + '/_matrix/client/versions', timeout=2) as response:
            if response.status == 200:
                break
    except OSError:
        time.sleep(1)
else:
    raise RuntimeError('Tuwunel did not become ready')


def register(username):
    body = json.dumps({
        'username': username,
        'password': 'local-ci-password-only',
        'auth': {'type': 'm.login.registration_token', 'token': 'local-ci-registration-only'},
    }).encode()
    request = urllib.request.Request(
        url + '/_matrix/client/v3/register', data=body, method='POST',
        headers={'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


worker_user = register('worker-ci')
sender_user = register('sender-ci')
worker = MatrixClient(url, worker_user['access_token'])
sender = MatrixClient(url, sender_user['access_token'])
room_id = worker.request('POST', '/_matrix/client/v3/createRoom', {'visibility': 'private'})['room_id']
room = urllib.parse.quote(room_id, safe='')
worker.request('POST', f'/_matrix/client/v3/rooms/{room}/invite', {'user_id': sender_user['user_id']})
sender.request('POST', f'/_matrix/client/v3/join/{room}', {})
since = worker.sync(None, 0)['next_batch']
sent = [sender.send_text(room_id, f'task-{i:03d}', transaction_key=f'ci-{i}') for i in range(137)]
payload = worker.sync(since, 0)
timeline = payload['rooms']['join'][room_id]['timeline']
assert timeline.get('limited') is True, f'Expected a limited sync; got {len(timeline["events"])} events'
rooms = {room_id: 'personal'}
before = matrix_events(payload, rooms, worker_user['user_id'])
worker.fill_timeline_gaps(payload, since, rooms)
after = matrix_events(payload, rooms, worker_user['user_id'])
assert [event['event_id'] for event in after] == sent, (
    f'Expected {len(sent)} unique ordered task events; got {len(after)}'
)
print(f'Tuwunel limited sync: {len(before)} of {len(sent)} tasks before backfill; all {len(after)} recovered in order')
