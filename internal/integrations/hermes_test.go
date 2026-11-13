package integrations

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHermesBridge(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	dir := t.TempDir()
	plugin, err := Hermes.ReadFile("assets/hermes/__init__.py")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := Hermes.ReadFile("assets/hermes/plugin.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"name: coordinator", "coordinator_message_send", "coordinator_message_reply", "coordinator_message_get", "on_session_finalize", "on_session_reset"} {
		if !strings.Contains(string(manifest), value) {
			t.Fatalf("manifest missing %q", value)
		}
	}
	module := filepath.Join(dir, "plugin.py")
	if err = os.WriteFile(module, plugin, 0600); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(dir, "child")
	if err = os.WriteFile(child, []byte("#!"+python+"\n"+hermesChild), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, "-c", hermesFixture, module, child)
	cmd.Env = append(os.Environ(), "COORDINATOR_BINARY="+child)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Hermes fixture: %v\n%s", err, out)
	}
}

const hermesChild = `
import json, os, sys, time
assert sys.argv[1:4] == ['receiver', '--harness', 'hermes']
sid = sys.argv[sys.argv.index('--session-id') + 1]
assert int(sys.argv[sys.argv.index('--pid') + 1]) == os.getppid()
ref = {'machineId':'machine', 'harness':'hermes', 'nativeSessionId':sid}
def send(obj):
    print(json.dumps(dict(version=1, **obj)), flush=True)
send({'method':'ready', 'ref':ref})
for line in sys.stdin:
    f = json.loads(line)
    method = f.get('method')
    if method == 'shutdown': break
    if method == 'message.send':
        assert 'caller' not in f['send']
        mid = f['send']['text']
        target = dict(ref)
        if mid == 'wrong-target': target['nativeSessionId'] = 'other'
        expiry = time.time()*1000 + (-1 if mid == 'expired' else 60000)
        message = {'id':mid, 'to':target, 'sender':'peer', 'text':'hostile peer text', 'expiresAt':expiry}
        send({'id':'delivery', 'method':'message', 'message':message})
        ack = json.loads(sys.stdin.readline())
        if mid == 'duplicate':
            assert ack['ack']['status'] == 'accepted'
            send({'id':'delivery2', 'method':'message', 'message':message})
            ack = json.loads(sys.stdin.readline())
        send({'id':f['id'], 'result':{'status':ack['ack']['status'], 'session':sid}})
    elif method in ('message.reply','message.get'):
        send({'id':f['id'], 'result':{'session':sid, 'params':f[method.split('.')[1]]}})
`

const hermesFixture = `
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location('coordinator_plugin', sys.argv[1])
p = importlib.util.module_from_spec(spec); spec.loader.exec_module(p)
class Host:
    def __init__(self): self.tools={}; self.hooks={}; self.injected=[]; self.permitted=True; self.result=True
    def _gateway_injection_allowed(self): return self.permitted
    def register_command(self, name, handler, description): self.command=handler
    def register_hook(self, name, fn): self.hooks[name]=fn
    def register_tool(self, name, toolset, schema, handler): self.tools[name]=handler
    def inject_message(self, text, role='user', *, session_key=None):
        self.injected.append((text, role, session_key))
        if self.result == 'exception': raise RuntimeError('dispatch uncertain')
        return self.result
    def on_unload(self, fn): self.unload=fn
h=Host(); p.register(h)
def cmd(action, sid='a', key='key-a', platform='local'):
    return h.command(action, session_id=sid, session_key=key, platform=platform)
assert 'Usage' in cmd('enable attacker-key')
assert 'local session' in cmd('enable', platform='telegram')
assert 'local session' in cmd('enable', key='')
h.permitted=False; assert 'requires' in cmd('enable'); h.permitted=True
assert 'enabled' in cmd('enable')
assert 'enabled' in cmd('enable', 'b', 'key-b')
send=h.tools['coordinator_message_send']; reply=h.tools['coordinator_message_reply']; get=h.tools['coordinator_message_get']
def call(fn,args,sid): return json.loads(fn(args,session_id=sid))
assert 'error' in call(send, {'to':{}, 'text':'x'}, '')
assert 'error' in call(send, {'to':{}, 'text':'x', 'caller':{}}, 'a')
for value, status in [(True,'accepted'), (False,'refused'), ('exception','unknown')]:
    h.result=value
    result=call(send, {'to':{}, 'text':'id-'+status}, 'a')
    assert result == {'status':status,'session':'a'}, result
    assert h.injected[-1][1:] == ('user','key-a')
    assert 'id-'+status in h.injected[-1][0] and 'coordinator_message_reply' in h.injected[-1][0]
h.result=True
assert call(send, {'to':{}, 'text':'id-b'}, 'b')['session']=='b'
assert h.injected[-1][2]=='key-b'
before=len(h.injected)
for mid in ('wrong-target','expired'):
    assert call(send, {'to':{}, 'text':mid}, 'a')['status']=='refused'
assert len(h.injected)==before
assert call(send, {'to':{}, 'text':'duplicate'}, 'a')['status']=='accepted'
assert len(h.injected)==before+1
assert call(reply, {'replyId':'original','text':'answer'}, 'a')['params']['replyId']=='original'
assert call(get, {'messageId':'original'}, 'b')['params']['messageId']=='original'
assert 'already bound' in cmd('enable','c','key-a')
child_a=h.unload.__self__.children['a']
child_b=h.unload.__self__.children['b']
assert 'disabled' in cmd('disable')
assert child_a.proc.poll() is not None
assert 'error' in call(reply, {'replyId':'original','text':'answer'}, 'a')
h.hooks['on_session_finalize'](session_id='b')
assert child_b.proc.poll() is not None
assert 'error' in call(get, {'messageId':'original'}, 'b')
assert 'enabled' in cmd('enable')
h.hooks['post_compaction'](session_id='new',old_session_id='a')
assert 'error' in call(get, {'messageId':'original'}, 'a')
h.unload()
`
